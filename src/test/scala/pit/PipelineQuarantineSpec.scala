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

class PipelineQuarantineSpec extends AnyFunSuite with SparkTestSupport {

  private def cfgFor(root: String): AppConfig =
    AppConfig(
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

  test("Fail batch (duplicate natural key) writes quarantine/detail and throws, with no schema conflict") {
    withTempDir { root =>
      val cfg = cfgFor(root)
      // Two rows with an identical natural key (adsh,tag,version,ddate,qtrs) -> uniqueness Fail.
      val filings = Seq(
        Filing("a1", 1, "Assets", 20230331, 1, bd("100"), "2023-05-01 12:00:00", "2023-05-02 00:00:00"),
        Filing("a1", 1, "Assets", 20230331, 1, bd("200"), "2023-06-01 12:00:00", "2023-06-02 00:00:00")
      )
      val ex = intercept[DataQualityException] {
        Pipeline.runFromFrames(
          spark,
          cfg,
          num = Fixtures.numDf(spark, filings),
          sub = Fixtures.subDf(spark, filings),
          batchId = "b-fail",
          ingestTs = Timestamp.valueOf("2023-06-02 00:00:00")
        )
      }
      assert(ex.getMessage.startsWith("DQ gate failed"), ex.getMessage)

      val detail = spark.read.format("delta").load(s"${cfg.paths.quarantineRoot}/detail")
      assert(detail.count() > 0L)
    }
  }

  test("missing-value source writes quarantine/unevaluable and throws, distinct schema from detail") {
    withTempDir { root =>
      val cfg = cfgFor(root)
      import spark.implicits._
      // Bronze source WITHOUT the `value` column -> §6 source-presence precondition: Unevaluable.
      val num = Seq(("a1", "Assets", "v1", 20230331, 1, "USD"))
        .toDF("adsh", "tag", "version", "ddate", "qtrs", "uom")
      val sub = Fixtures.subDf(
        spark,
        Seq(Filing("a1", 1, "Assets", 20230331, 1, bd("100"), "2023-05-01 12:00:00", "2023-05-02 00:00:00"))
      )
      val ex = intercept[DataQualityException] {
        Pipeline.runFromFrames(spark, cfg, num, sub, "b-uneval", Timestamp.valueOf("2023-05-02 00:00:00"))
      }
      assert(ex.getMessage.toLowerCase.contains("unevaluable"), ex.getMessage)

      val uneval = spark.read.format("delta").load(s"${cfg.paths.quarantineRoot}/unevaluable")
      assert(uneval.columns.toSet == Set("batchId", "reason"))
      assert(uneval.filter(uneval("batchId") === "b-uneval").count() == 1L)
    }
  }
}
