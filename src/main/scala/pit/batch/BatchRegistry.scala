package pit.batch

import io.delta.tables.DeltaTable
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

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
      spark.sparkContext.parallelize(
        Seq(Row(record.batchId, record.manifestJson, record.deltaVersion))
      ),
      schema
    )
    if (!DeltaTable.isDeltaTable(spark, registryPath)) {
      row.write.format("delta").mode("overwrite").save(registryPath)
    } else {
      DeltaTable
        .forPath(spark, registryPath)
        .as("t")
        .merge(row.as("s"), "t.batch_id = s.batch_id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
  }

  def lookup(spark: SparkSession, registryPath: String, batchId: String): Option[BatchRecord] =
    if (!DeltaTable.isDeltaTable(spark, registryPath)) None
    else
      spark.read
        .format("delta")
        .load(registryPath)
        .filter(col("batch_id") === batchId)
        .collect()
        .headOption
        .map(r => BatchRecord(r.getString(0), r.getString(1), r.getLong(2)))
}
