package pit.tools

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.countDistinct
import pit.Pipeline
import pit.config.AppConfig
import pit.config.IngestCfg
import pit.config.PathsCfg
import pit.config.SparkCfg
import pit.support.Fixtures
import pit.support.Fixtures.Filing

import java.sql.Timestamp

/** Builds a REAL gold Delta table for the dbt CI fixture by driving `Pipeline.runFromFrames` -- the same
  * production path `PipelineRestatementSpec` proves -- over synthesized filings, so CI's dbt run reads
  * genuine pipeline output instead of a hand-written parquet.
  *
  * Invoked as: `sbt "Test/runMain pit.tools.DbtFixtureMain <output-lake-root>"`. Gold lands at
  * `<output-lake-root>/gold`, matching WS2's `ci` target (`$GITHUB_WORKSPACE/ci-fixture/gold`).
  */
object DbtFixtureMain {

  // Two batches, so gold captures a real restatement: rows accepted/ingested in the base batch,
  // then a second batch, later on both axes, that corrects two of those natural keys.
  private val baseAccepted = "2023-08-01 09:00:00"
  private val baseIngest = "2023-08-01 09:00:00"
  private val restatedAccepted = "2023-11-01 09:00:00"
  private val restatedIngest = "2023-11-01 09:00:00"

  private val ciks: Seq[Long] = Seq(1001L, 1002L, 1003L)
  private val tags: Seq[String] = Seq("Assets", "Liabilities", "Revenues")

  private final case class Period(ddate: Int, qtrs: Int)
  private val periods: Seq[Period] =
    Seq(Period(20230331, 1), Period(20230630, 1), Period(20221231, 4))

  /** Natural keys restated in the second batch: same (cik, tag, ddate, qtrs, uom), a new adsh, a later
    * `accepted` -- exactly the shape `restatement_freq` exists to surface and `as_of_facts` has to choose
    * between.
    */
  private val restatedKeys: Seq[(Long, String, Period)] = Seq(
    (1001L, "Assets", Period(20230331, 1)),
    (1002L, "Revenues", Period(20230630, 1))
  )

  private def baseFilings(): Seq[Filing] = {
    val combos = for {
      cik <- ciks
      tag <- tags
      period <- periods
    } yield (cik, tag, period)
    combos.zipWithIndex.map { case ((cik, tag, period), idx) =>
      Filing(
        adsh = s"base-$idx",
        cik = cik,
        tag = tag,
        ddate = period.ddate,
        qtrs = period.qtrs,
        value = Fixtures.bd(s"${100 + idx * 10}.0000"),
        accepted = baseAccepted,
        ingest = baseIngest
      )
    }
  }

  private def restatementFilings(): Seq[Filing] =
    restatedKeys.zipWithIndex.map { case ((cik, tag, period), idx) =>
      Filing(
        adsh = s"restated-$idx",
        cik = cik,
        tag = tag,
        ddate = period.ddate,
        qtrs = period.qtrs,
        value = Fixtures.bd(s"${999 + idx * 10}.0000"),
        accepted = restatedAccepted,
        ingest = restatedIngest
      )
    }

  private def cfgFor(root: String): AppConfig =
    AppConfig(
      SparkCfg("dbt-fixture", Some("local[2]")),
      PathsCfg(
        s"$root/src",
        s"$root/bronze",
        s"$root/silver",
        s"$root/gold",
        s"$root/quarantine",
        s"$root/registry"
      ),
      IngestCfg(List("t"))
    )

  private def ingestBatch(
      spark: SparkSession,
      cfg: AppConfig,
      batchId: String,
      fs: Seq[Filing],
      ingestTs: Timestamp
  ): Unit = {
    Pipeline.runFromFrames(
      spark,
      cfg,
      num = Fixtures.numDf(spark, fs),
      sub = Fixtures.subDf(spark, fs),
      batchId = batchId,
      ingestTs = ingestTs
    )
    ()
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      Console.err.println("DbtFixtureMain: usage: DbtFixtureMain <output-lake-root>")
      sys.exit(1)
    }
    val root = args(0)
    val cfg = cfgFor(root)

    // Replicates SparkTestSupport's session config (Delta extensions + catalog): this is a main,
    // not a Suite, so that trait is unavailable here.
    val spark = SparkSession
      .builder()
      .appName("dbt-fixture")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    // SEC `accepted` timestamps are zoneless US Eastern; pin the session so this fixture's PIT
    // boundary matches the production pipeline's, not the CI runner's local zone.
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")

    try {
      ingestBatch(spark, cfg, "ci-fixture-base", baseFilings(), Timestamp.valueOf(baseIngest))
      ingestBatch(
        spark,
        cfg,
        "ci-fixture-restated",
        restatementFilings(),
        Timestamp.valueOf(restatedIngest)
      )

      val gold = spark.read.format("delta").load(cfg.paths.goldRoot)
      val rowCount = gold.count()
      if (rowCount == 0L) {
        // A fixture builder that silently produces an empty gold table would make every
        // downstream dbt test pass vacuously -- fail closed instead.
        Console.err.println("DbtFixtureMain: gold table is empty, refusing a vacuous fixture")
        sys.exit(1)
      }
      val distinctCiks = gold.select(col("cik")).distinct().count()
      val restatedKeyCount = gold
        .groupBy(col("cik"), col("tag"), col("ddate"), col("qtrs"), col("uom"))
        .agg(countDistinct(col("adsh")).as("adsh_count"))
        .filter(col("adsh_count") > 1)
        .count()

      println(s"DbtFixtureMain: wrote gold at $root/gold")
      println(
        s"DbtFixtureMain: rows=$rowCount distinct_ciks=$distinctCiks restated_keys=$restatedKeyCount"
      )
    } finally spark.stop()
  }
}
