package pit.silver

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import pit.config.AppConfig

/** Accumulated typed filing index (adsh -> cik, accepted), one row per adsh, grown batch by batch. Gold must
  * always rebuild against THIS store, never a single batch's sub slice: gold's adsh join scoped to the
  * current batch silently drops every previously ingested filing's facts from the overwritten gold table (the
  * gold sub-scoping finding, 2026-07-07). Same tiebreak contract as SilverMerge: latest _ingest_ts wins,
  * equal is a no-op.
  */
object SilverSubStore {
  def path(cfg: AppConfig): String = s"${cfg.paths.silverRoot}_sub"

  def upsert(spark: SparkSession, storePath: String, batch: DataFrame): Unit =
    if (!DeltaTable.isDeltaTable(spark, storePath)) {
      batch.write.format("delta").mode("overwrite").save(storePath)
    } else {
      DeltaTable
        .forPath(spark, storePath)
        .as("t")
        .merge(batch.as("s"), "t.adsh = s.adsh")
        .whenMatched("s._ingest_ts > t._ingest_ts")
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
}
