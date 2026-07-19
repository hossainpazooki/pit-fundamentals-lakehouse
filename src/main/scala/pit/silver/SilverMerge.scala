package pit.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import pit.util.DeltaIO

object SilverMerge {
  private val keyCols = Seq("adsh", "tag", "version", "ddate", "qtrs", "uom")

  /** Restatement upsert. Idempotent: latest _ingest_ts wins; equal is a no-op. */
  def upsert(spark: SparkSession, silverPath: String, batch: DataFrame): Unit =
    if (!DeltaIO.isDeltaTable(spark, silverPath)) {
      batch.write.format("delta").mode("overwrite").save(silverPath)
    } else {
      val cond = keyCols.map(c => s"t.$c = s.$c").mkString(" AND ")
      DeltaIO.merge(
        spark,
        silverPath,
        batch,
        cond,
        matchedUpdateCondition = Some("s._ingest_ts > t._ingest_ts")
      )
    }
}
