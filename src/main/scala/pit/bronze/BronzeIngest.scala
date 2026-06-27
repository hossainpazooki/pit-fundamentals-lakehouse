package pit.bronze

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.sql.Timestamp

object BronzeIngest {

  /** Header-named read, inference banned. Projects declared columns present by name. */
  def readTsv(spark: SparkSession, path: String, declaredColumns: Seq[String]): DataFrame = {
    val raw = spark.read
      .option("header", "true")
      .option("sep", "\t")
      .option("inferSchema", "false")
      .csv(path)
    val present = declaredColumns.filter(raw.columns.contains)
    raw.select(present.map(col): _*)
  }

  def withAuditColumns(
      df: DataFrame,
      batchId: String,
      sourceFile: String,
      ingestTs: Timestamp
  ): DataFrame = {
    // Row hash over all source columns in declared order, so re-reads are stable.
    val concatExpr = concat_ws("", df.columns.map(c => coalesce(col(c).cast("string"), lit(" "))): _*)
    df.withColumn("_ingest_ts", lit(ingestTs))
      .withColumn("_source_file", lit(sourceFile))
      .withColumn("_batch_id", lit(batchId))
      .withColumn("_src_row_hash", sha2(concatExpr, 256))
  }

  /** Append-only. Dedupe on (_batch_id, _src_row_hash) so re-ingest is a no-op. */
  def appendIdempotent(spark: SparkSession, bronzePath: String, batch: DataFrame): Unit =
    if (!DeltaTable.isDeltaTable(spark, bronzePath)) {
      batch.write.format("delta").mode("overwrite").save(bronzePath)
    } else {
      val table = DeltaTable.forPath(spark, bronzePath)
      table
        .as("t")
        .merge(
          batch.as("s"),
          "t._batch_id = s._batch_id AND t._src_row_hash = s._src_row_hash"
        )
        .whenNotMatched()
        .insertAll()
        .execute()
    }
}
