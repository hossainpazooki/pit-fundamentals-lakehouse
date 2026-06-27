package pit

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import pit.config.AppConfig
import pit.gold.GoldPit
import pit.observability.RunMetrics
import pit.silver.DataQualityGate
import pit.silver.Fail
import pit.silver.Pass
import pit.silver.SilverMerge
import pit.silver.SilverTransforms
import pit.silver.Unevaluable

import java.sql.Timestamp

class DataQualityException(msg: String) extends RuntimeException(msg)

object Pipeline {

  /** Core stage path, frame-driven so it unit-tests without reading TSVs. */
  def runFromFrames(
      spark: SparkSession,
      cfg: AppConfig,
      num: DataFrame,
      sub: DataFrame,
      batchId: String,
      ingestTs: Timestamp
  ): RunMetrics = {
    val rowsIn = num.count()
    // §6 precondition on the SOURCE: a missing required column must be Unevaluable, not masked
    // into a completeness Fail by castNum (which manufactures absent columns as typed nulls).
    DataQualityGate.requiredColumnsPresent(num, DataQualityGate.requiredNumCols).foreach { reason =>
      throw new DataQualityException(s"DQ gate unevaluable: $reason")
    }
    val casted = SilverTransforms.castNum(num)
    val (valid, quarantine) =
      SilverTransforms.splitValidQuarantine(casted, Seq("adsh", "tag", "version", "ddate", "qtrs"))
    val quarantined = quarantine.count()
    if (quarantined > 0)
      quarantine.write.format("delta").mode("append").save(cfg.paths.quarantineRoot)

    val outcome =
      DataQualityGate.gate(valid, DataQualityGate.silverNumContract, DataQualityGate.requiredNumCols)
    val gateLabel = outcome match {
      case Pass => "Pass"
      case Fail(detail) =>
        detail.write.format("delta").mode("append").save(cfg.paths.quarantineRoot)
        throw new DataQualityException(
          "DQ gate failed: " + detail.take(50).map(_.toString).mkString("; ")
        )
      case Unevaluable(why) =>
        throw new DataQualityException(s"DQ gate unevaluable: $why")
    }

    SilverMerge.upsert(spark, cfg.paths.silverRoot, valid)
    val silver = spark.read.format("delta").load(cfg.paths.silverRoot)
    val gold = GoldPit.buildGold(silver, sub)
    gold.write.format("delta").mode("overwrite").save(cfg.paths.goldRoot)

    val deltaVersion =
      io.delta.tables.DeltaTable.forPath(spark, cfg.paths.goldRoot).history(1).head().getAs[Long]("version")
    RunMetrics(batchId, rowsIn, gold.count(), rowsIn - valid.count(), quarantined, gateLabel, deltaVersion)
  }

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()
    val spark = SparkSession
      .builder()
      .appName(cfg.spark.appName)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    try
      // Full TSV read path: bronze ingest per quarter, then silver+gold.
      // (Read/audit/registry wiring uses BronzeIngest + BatchRegistry from Tasks 4-5.)
      println("pipeline configured for quarters: " + cfg.ingest.quarters.mkString(","))
    finally spark.stop()
  }
}
