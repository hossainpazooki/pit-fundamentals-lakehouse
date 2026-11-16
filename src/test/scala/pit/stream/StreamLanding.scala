package pit.stream

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import pit.DataQualityException
import pit.bronze.BronzeIngest
import pit.common.Schemas
import pit.config.AppConfig
import pit.silver.DataQualityGate

import java.sql.Timestamp

/** Lands one micro-batch of num.txt line events in bronze through the SAME functions the file route uses
  * (`BronzeIngest.withAuditColumns` + `appendIdempotent`), so a redelivered record is a no-op on `(_batch_id,
  * _src_row_hash)`. Fail-closed: any malformed record refuses the WHOLE micro-batch -- the refused records go
  * to their own quarantine lane and the call throws, which fails the streaming query before its checkpoint
  * advances. Test scope by design (WS3): the shipped jars carry no streaming code.
  *
  * Event shape (`eventColumns`): `value` is one raw num.txt data line; `tsv_header` is that file's header
  * line (the read stays header-named, inference banned); `quarter` and `source_sha256` are producer-declared
  * and only compared here, never recomputed.
  */
object StreamLanding {

  val eventColumns: Seq[String] =
    Seq("value", "tsv_header", "quarter", "source_sha256", "topic", "partition", "offset")

  /** Its own Delta path: the schema differs from the rows / detail / unevaluable lanes. */
  def quarantinePath(cfg: AppConfig): String = s"${cfg.paths.quarantineRoot}/stream"

  private def refuse(cfg: AppConfig, batchId: String, rejected: DataFrame, summary: String): Nothing = {
    rejected
      .select(col("topic"), col("partition"), col("offset"), col("value"), col("reason"))
      .withColumn("batchId", lit(batchId))
      .write
      .format("delta")
      .mode("append")
      .save(quarantinePath(cfg))
    throw new DataQualityException(s"stream landing refused: $summary")
  }

  /** @param spark
    *   the session of `events` itself (`df.sparkSession` inside `foreachBatch`): `DeltaIO.merge` registers a
    *   temp view on the frame's session and runs SQL on this one, so they must be the same session.
    * @return
    *   records landed (redelivered records count here and are no-ops in bronze)
    */
  def landMicroBatch(
      spark: SparkSession,
      cfg: AppConfig,
      events: DataFrame,
      expectedQuarter: String,
      expectedSha: String,
      batchId: String,
      sourceLabel: String,
      ingestTs: Timestamp
  ): Long = {
    val batch = events.select(eventColumns.map(col): _*).persist()
    try {
      val total = batch.count()
      if (total == 0L) 0L
      else {
        val headers = batch.select(col("tsv_header")).distinct().collect().map(r => Option(r.getString(0)))
        val header = headers match {
          case Array(Some(h)) if h.trim.nonEmpty => h
          case _ =>
            val why = "tsv_header missing or not unique in the micro-batch"
            refuse(cfg, batchId, batch.withColumn("reason", lit(why)), s"$why ($total record(s))")
        }
        val names = header.split("\t", -1).toSeq
        val reason = when(col("value").isNull, lit("null value"))
          .when(
            col("quarter").isNull || col("quarter") =!= lit(expectedQuarter),
            lit("quarter header missing or unexpected")
          )
          .when(
            col("source_sha256").isNull || col("source_sha256") =!= lit(expectedSha),
            lit("source_sha256 header missing or unexpected")
          )
          .when(
            size(split(col("value"), "\t", -1)) =!= lit(names.length),
            lit(s"field count is not ${names.length}")
          )
        val bad = batch.withColumn("reason", reason).filter(col("reason").isNotNull)
        val badCount = bad.count()
        if (badCount > 0L) {
          val first = bad.orderBy(col("offset")).select(col("reason")).head().getString(0)
          refuse(cfg, batchId, bad, s"$badCount malformed record(s) of $total; first reason: $first")
        }
        // Same parser and options as BronzeIngest.readTsv (univocity, sep = tab, all strings).
        val schema = StructType(names.map(n => StructField(n, StringType)))
        val parsed =
          batch.select(from_csv(col("value"), schema, Map("sep" -> "\t")).as("r")).select(col("r.*"))
        DataQualityGate
          .requiredColumnsPresent(parsed, DataQualityGate.requiredNumCols ++ DataQualityGate.scopeCols)
          .foreach(why => refuse(cfg, batchId, batch.withColumn("reason", lit(why)), why))
        val declared = parsed.select(Schemas.numColumns.filter(parsed.columns.contains).map(col): _*)
        BronzeIngest.appendIdempotent(
          spark,
          s"${cfg.paths.bronzeRoot}/num",
          BronzeIngest.withAuditColumns(declared, batchId, sourceLabel, ingestTs)
        )
        total
      }
    } finally {
      batch.unpersist()
      ()
    }
  }
}
