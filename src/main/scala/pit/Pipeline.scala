package pit

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import pit.config.AppConfig
import pit.gold.GoldRebuild
import pit.observability.RunMetrics
import pit.silver.DataQualityGate
import pit.silver.Fail
import pit.silver.Pass
import pit.silver.SilverMerge
import pit.silver.SilverSubStore
import pit.silver.SilverTransforms
import pit.silver.Unevaluable

import java.sql.Timestamp

class DataQualityException(msg: String) extends RuntimeException(msg)

object Pipeline {

  /** Write the single-row `(batchId, reason)` provenance for an unevaluable batch, then fail closed.
    * Different from the row/detail quarantine schemas, so it gets its own Delta path -- one path with two
    * schemas throws on the second write.
    */
  private def writeUnevaluableAndThrow(
      spark: SparkSession,
      cfg: AppConfig,
      batchId: String,
      reason: String
  ): Nothing = {
    import spark.implicits._
    Seq((batchId, reason))
      .toDF("batchId", "reason")
      .write
      .format("delta")
      .mode("append")
      .save(s"${cfg.paths.quarantineRoot}/unevaluable")
    throw new DataQualityException(s"DQ gate unevaluable: $reason")
  }

  /** Core stage path, frame-driven so it unit-tests without reading TSVs: silver stages plus a gold rebuild
    * from the ACCUMULATED sub store (never this batch's sub slice — the gold sub-scoping finding,
    * 2026-07-07).
    */
  def runFromFrames(
      spark: SparkSession,
      cfg: AppConfig,
      num: DataFrame,
      sub: DataFrame,
      batchId: String,
      ingestTs: Timestamp
  ): RunMetrics = {
    val m = runSilverStage(spark, cfg, num, sub, batchId, ingestTs)
    GoldRebuild.rebuild(spark, cfg)
    m
  }

  /** Stages through the silver merge only, so a multi-quarter backfill can defer the gold rebuild to one
    * `GoldRebuild` pass at the end. Metrics are silver-stage: `rowsOut` is rows merged to silver (was: gold
    * table count) and `deltaVersion` is the silver table version (was: gold).
    */
  def runSilverStage(
      spark: SparkSession,
      cfg: AppConfig,
      num: DataFrame,
      sub: DataFrame,
      batchId: String,
      ingestTs: Timestamp
  ): RunMetrics = {
    val startNanos = System.nanoTime()
    val rowsIn = num.count()
    // §6 precondition on the SOURCE: a missing required column must be Unevaluable, not masked
    // into a completeness Fail by castNum (which manufactures absent columns as typed nulls).
    // The scope columns are required too: without them entity-level scope cannot be established.
    DataQualityGate
      .requiredColumnsPresent(num, DataQualityGate.requiredNumCols ++ DataQualityGate.scopeCols)
      .foreach { reason =>
        writeUnevaluableAndThrow(spark, cfg, batchId, reason)
      }
    // Entity-level scope: segment/coregistrant breakouts are excluded by design and counted
    // in metrics, not quarantined (they are valid SEC rows outside VANTAGE's claim).
    val (entity, scopedOutDf) = SilverTransforms.scopeEntityLevel(num)
    val scopedOut = scopedOutDf.count()
    val casted = SilverTransforms.castNum(entity)
    // `value` is structural for a numeric fact store: footnote-only rows (null value) go to
    // the rows lane rather than failing the whole batch on completeness.
    val (valid, quarantine) =
      SilverTransforms.splitValidQuarantine(
        casted,
        Seq("adsh", "tag", "version", "ddate", "qtrs", "uom", "value")
      )
    val quarantined = quarantine.count()
    if (quarantined > 0)
      quarantine.write.format("delta").mode("append").save(s"${cfg.paths.quarantineRoot}/rows")

    val outcome =
      DataQualityGate.gate(valid, DataQualityGate.silverNumContract, DataQualityGate.requiredNumCols)
    val gateLabel = outcome match {
      case Pass => "Pass"
      case Fail(detail) =>
        detail.write.format("delta").mode("append").save(s"${cfg.paths.quarantineRoot}/detail")
        throw new DataQualityException(
          "DQ gate failed: " + detail.take(50).map(_.toString).mkString("; ")
        )
      case Unevaluable(why) =>
        writeUnevaluableAndThrow(spark, cfg, batchId, why)
    }

    SilverMerge.upsert(spark, cfg.paths.silverRoot, valid)
    // Grow the accumulated filing index in lockstep with silver, so any later gold rebuild joins
    // against every filing ever ingested, not this batch's slice.
    SilverSubStore.upsert(
      spark,
      SilverSubStore.path(cfg),
      sub.withColumn("_ingest_ts", lit(ingestTs))
    )

    val deltaVersion =
      io.delta.tables.DeltaTable.forPath(spark, cfg.paths.silverRoot).history(1).head().getAs[Long]("version")
    RunMetrics(
      batchId,
      rowsIn,
      valid.count(),
      scopedOut,
      rowsIn - scopedOut - valid.count(),
      quarantined,
      gateLabel,
      deltaVersion,
      (System.nanoTime() - startNanos) / 1000000L
    )
  }

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()
    val builder = SparkSession
      .builder()
      .appName(cfg.spark.appName)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // SEC `accepted` timestamps are US Eastern and carry no zone; parsing them in the
      // machine's local zone would move the PIT boundary across machines (found on the
      // 2026q1 run, GATE-A record). Pin the session zone so ingest is zone-deterministic.
      .config("spark.sql.session.timeZone", "America/New_York")
    cfg.spark.master.foreach(builder.master) // absent on a cluster, where the runtime provides it
    val spark = builder.getOrCreate()
    try {
      val codeSha = sys.env.getOrElse("VANTAGE_CODE_SHA", "unknown")
      val ingestTs = new java.sql.Timestamp(System.currentTimeMillis())
      // Silver stage only: gold is a pure rebuild over the WHOLE lake, so a multi-quarter run
      // defers it to one `pit.gold.GoldRebuild` pass at the end instead of rebuilding an
      // ever-growing gold once per quarter. Single-shot local runs: run GoldRebuild after this.
      cfg.ingest.quarters.foreach { q =>
        val r = pit.ingest.TsvIngest.ingestQuarter(spark, cfg, q, codeSha, ingestTs)
        val m = runSilverStage(spark, cfg, r.num, r.sub, r.batchId, ingestTs)
        println(m.asLogLine) // ASCII
      }
    } finally spark.stop()
  }
}
