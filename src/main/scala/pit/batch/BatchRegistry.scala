package pit.batch

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import pit.util.DeltaIO

final case class BatchRecord(batchId: String, manifestJson: String, deltaVersion: Long)

object BatchRegistry {
  private val schema = StructType(
    Seq(
      StructField("batch_id", StringType, nullable = false),
      StructField("manifest_json", StringType, nullable = false),
      StructField("delta_version", LongType, nullable = false)
    )
  )

  def register(spark: SparkSession, registryPath: String, record: BatchRecord): Unit = {
    val row = spark.createDataFrame(
      java.util.Collections.singletonList(
        Row(record.batchId, record.manifestJson, record.deltaVersion)
      ),
      schema
    )
    if (!DeltaIO.isDeltaTable(spark, registryPath)) {
      row.write.format("delta").mode("overwrite").save(registryPath)
    } else {
      DeltaIO.merge(spark, registryPath, row, "t.batch_id = s.batch_id")
    }
  }

  def lookup(spark: SparkSession, registryPath: String, batchId: String): Option[BatchRecord] =
    if (!DeltaIO.isDeltaTable(spark, registryPath)) None
    else
      spark.read
        .format("delta")
        .load(registryPath)
        .filter(col("batch_id") === batchId)
        .collect()
        .headOption
        .map(r => BatchRecord(r.getString(0), r.getString(1), r.getLong(2)))
}
