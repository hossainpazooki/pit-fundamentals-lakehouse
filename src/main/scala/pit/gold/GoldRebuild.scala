package pit.gold

import org.apache.spark.sql.SparkSession
import pit.config.AppConfig
import pit.silver.SilverSubStore
import pit.util.DeltaIO

/** Gold is a PURE REBUILD: full silver joined to the ACCUMULATED filing index. Building gold from a single
  * batch's sub frame scopes the adsh join to that batch and drops every other filing's facts from the
  * overwritten table (the gold sub-scoping finding, 2026-07-07).
  */
object GoldRebuild {

  /** Rebuild gold from current silver + the accumulated sub store; returns the new gold version. */
  def rebuild(spark: SparkSession, cfg: AppConfig): Long = {
    val silver = spark.read.format("delta").load(cfg.paths.silverRoot)
    val subAll = spark.read.format("delta").load(SilverSubStore.path(cfg)).drop("_ingest_ts")
    val gold = GoldPit.buildGold(silver, subAll)
    gold.write.format("delta").mode("overwrite").save(cfg.paths.goldRoot)
    DeltaIO.latestVersion(spark, cfg.paths.goldRoot)
  }

  /** Standalone entry point: one gold rebuild at the end of a multi-quarter backfill, instead of one rebuild
    * per quarter over an ever-growing silver.
    */
  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.loadFromArgs(args)
    val builder = SparkSession.builder().appName(s"${cfg.spark.appName}-gold-rebuild")
    // Local/classic mode wires Delta itself; Databricks provides Delta, and Spark Connect
    // (serverless) rejects these static confs, so they ride the master setting (local-only).
    cfg.spark.master.foreach { m =>
      builder
        .master(m)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    }
    val spark = builder.getOrCreate()
    // Same zone pin as Pipeline.main: any timestamp cast here must not move the PIT boundary.
    // Runtime session conf, not builder conf, so the pin also applies on Spark Connect.
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    try {
      val t0 = System.nanoTime()
      val version = rebuild(spark, cfg)
      val goldRows = spark.read.format("delta").load(cfg.paths.goldRoot).count()
      val wallMs = (System.nanoTime() - t0) / 1000000L
      println(s"gold_rebuild rows=$goldRows delta_version=$version wall_clock_ms=$wallMs") // ASCII
    } finally spark.stop()
  }
}
