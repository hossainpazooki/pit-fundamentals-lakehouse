package pit

import org.scalatest.funsuite.AnyFunSuite
import pit.config.AppConfig
import pit.config.IngestCfg
import pit.config.PathsCfg
import pit.config.SparkCfg
import pit.support.Fixtures
import pit.support.Fixtures.Filing
import pit.support.Fixtures.bd
import pit.support.SparkTestSupport

import java.sql.Timestamp

class PipelineSpec extends AnyFunSuite with SparkTestSupport {
  test("end-to-end: bronze->silver->gold yields a PIT-correct gold table and metrics") {
    withTempDir { root =>
      val cfg = AppConfig(
        SparkCfg("t", Some("local[2]")),
        PathsCfg(
          s"$root/src",
          s"$root/bronze",
          s"$root/silver",
          s"$root/gold",
          s"$root/quarantine",
          s"$root/registry"
        ),
        IngestCfg(List("2023q2"))
      )
      val filings = Seq(
        Filing("a1", 1, "Assets", 20230331, 1, bd("100"), "2023-05-01 12:00:00", "2023-05-02 00:00:00"),
        Filing("a2", 1, "Assets", 20230331, 1, bd("150"), "2023-08-01 12:00:00", "2023-08-02 00:00:00")
      )
      // Seed silver + sub directly (pipeline read path is exercised by per-stage tests).
      val m = Pipeline.runFromFrames(
        spark,
        cfg,
        num = Fixtures.numDf(spark, filings),
        sub = Fixtures.subDf(spark, filings),
        batchId = "b-test",
        ingestTs = Timestamp.valueOf("2023-08-02 00:00:00")
      )

      assert(m.gateOutcome == "Pass")
      val gold = spark.read.format("delta").load(cfg.paths.goldRoot)
      assert(gold.count() == 2L)
    }
  }

  test("missing required column in source -> Unevaluable end-to-end (not masked into Fail by castNum)") {
    withTempDir { root =>
      val cfg = AppConfig(
        SparkCfg("t", Some("local[2]")),
        PathsCfg(
          s"$root/src",
          s"$root/bronze",
          s"$root/silver",
          s"$root/gold",
          s"$root/quarantine",
          s"$root/registry"
        ),
        IngestCfg(List("2023q2"))
      )
      import spark.implicits._
      // Bronze source WITHOUT the `value` column — the case castNum would otherwise mask.
      val num = Seq(("a1", "Assets", "v1", 20230331, 1, "USD"))
        .toDF("adsh", "tag", "version", "ddate", "qtrs", "uom")
      val sub = Fixtures.subDf(
        spark,
        Seq(Filing("a1", 1, "Assets", 20230331, 1, bd("100"), "2023-05-01 12:00:00", "2023-05-02 00:00:00"))
      )
      val ex = intercept[DataQualityException] {
        Pipeline.runFromFrames(spark, cfg, num, sub, "b-miss", Timestamp.valueOf("2023-05-02 00:00:00"))
      }
      assert(ex.getMessage.toLowerCase.contains("unevaluable"), ex.getMessage)
      assert(ex.getMessage.contains("value"), ex.getMessage)
    }
  }
}
