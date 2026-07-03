package pit.silver

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object SilverMerge {
  private val keyCols = Seq("adsh", "tag", "version", "ddate", "qtrs", "uom")

  /** Restatement upsert. Idempotent: latest _ingest_ts wins; equal is a no-op. */
  def upsert(spark: SparkSession, silverPath: String, batch: DataFrame): Unit =
    if (!DeltaTable.isDeltaTable(spark, silverPath)) {
      batch.write.format("delta").mode("overwrite").save(silverPath)
    } else {
      val cond = keyCols.map(c => s"t.$c = s.$c").mkString(" AND ")
      DeltaTable
        .forPath(spark, silverPath)
        .as("t")
        .merge(batch.as("s"), cond)
        .whenMatched("s._ingest_ts > t._ingest_ts")
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
}
