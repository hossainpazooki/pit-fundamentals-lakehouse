package pit.stream

import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import pit.DataQualityException
import pit.bronze.BronzeIngest
import pit.common.Schemas
import pit.config.AppConfig
import pit.config.IngestCfg
import pit.config.PathsCfg
import pit.config.SparkCfg
import pit.support.SparkTestSupport
import pit.util.DeltaIO

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Timestamp

class StreamLandingSpec extends AnyFunSuite with SparkTestSupport {

  private val header = "adsh\ttag\tversion\tddate\tqtrs\tuom\tsegments\tcoreg\tvalue\tfootnote"
  private val lines = Seq(
    "0001-09-000001\tAssets\tus-gaap/2008\t20090331\t0\tUSD\t\t\t13227000000.0000\t",
    "0001-09-000001\tLiabilities\tus-gaap/2008\t20090331\t0\tUSD\t\t\t9000000.0000\tsee \"note 4\"",
    "0001-09-000002\tTreasuryStockSharesAcquired\tus-gaap/2008\t20080331\t1\tshares\tEquityComponents=CommonStock;\t\t104000.0000\t"
  )
  private val sha = "a" * 64
  private val quarter = "2009q2"
  private val label = "kafka:vantage.num.v1"
  private val ts = Timestamp.valueOf("2023-04-01 00:00:00")

  private def cfgFor(root: String): AppConfig =
    AppConfig(
      SparkCfg("stream-test", Some("local[2]")),
      PathsCfg(
        s"$root/src",
        s"$root/bronze",
        s"$root/silver",
        s"$root/gold",
        s"$root/quarantine",
        s"$root/registry"
      ),
      IngestCfg(List(quarter))
    )

  private def events(values: Seq[String], hdr: String = header): DataFrame = {
    import spark.implicits._
    values.zipWithIndex
      .map { case (v, i) => (v, hdr, quarter, sha, "vantage.num.v1", 0, i.toLong) }
      .toDF("value", "tsv_header", "quarter", "source_sha256", "topic", "partition", "offset")
  }

  private def land(cfg: AppConfig, df: DataFrame): Long =
    StreamLanding.landMicroBatch(spark, cfg, df, quarter, sha, "b1", label, ts)

  test("well-formed events land with the file route's row hashes; redelivery is a no-op") {
    withTempDir { dir =>
      val cfg = cfgFor(dir)
      assert(land(cfg, events(lines)) == 3L)
      assert(land(cfg, events(lines)) == 3L) // the same micro-batch delivered again
      val bronze = spark.read.format("delta").load(s"${cfg.paths.bronzeRoot}/num")
      assert(bronze.count() == 3L, "redelivery must not add rows")

      Files.write(
        Paths.get(s"$dir/num.txt"),
        (header +: lines).mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8)
      )
      val fileRoute = BronzeIngest.withAuditColumns(
        BronzeIngest.readTsv(spark, s"$dir/num.txt", Schemas.numColumns),
        "b1",
        "num.txt",
        ts
      )
      val fileHashes = fileRoute.select("_src_row_hash").collect().map(_.getString(0)).toSet
      val streamHashes = bronze.select("_src_row_hash").collect().map(_.getString(0)).toSet
      assert(fileHashes.size == 3)
      assert(streamHashes == fileHashes, "stream landing must reproduce the file route's row hashes")
    }
  }

  test("a record with the wrong field count halts the micro-batch; nothing lands") {
    withTempDir { dir =>
      val cfg = cfgFor(dir)
      val bad = lines.updated(1, lines(1).split("\t", -1).dropRight(2).mkString("\t"))
      val e = intercept[DataQualityException](land(cfg, events(bad)))
      assert(e.getMessage.contains("field count"))
      val q = spark.read.format("delta").load(StreamLanding.quarantinePath(cfg))
      assert(q.count() == 1L)
      assert(q.select("offset").head().getLong(0) == 1L)
      assert(
        !DeltaIO.isDeltaTable(spark, s"${cfg.paths.bronzeRoot}/num"),
        "no row of a refused micro-batch may land"
      )
    }
  }

  test("a missing tsv_header refuses every record of the micro-batch") {
    withTempDir { dir =>
      val cfg = cfgFor(dir)
      val e = intercept[DataQualityException](land(cfg, events(lines, hdr = null)))
      assert(e.getMessage.contains("tsv_header"))
      assert(spark.read.format("delta").load(StreamLanding.quarantinePath(cfg)).count() == 3L)
      assert(!DeltaIO.isDeltaTable(spark, s"${cfg.paths.bronzeRoot}/num"))
    }
  }

  test("a header without a required column is refused, not landed with a manufactured null") {
    withTempDir { dir =>
      val cfg = cfgFor(dir)
      val noValueHeader = "adsh\ttag\tversion\tddate\tqtrs\tuom\tsegments\tcoreg\tfootnote"
      val noValueLines = lines.map { l =>
        val f = l.split("\t", -1)
        (f.take(8) ++ f.drop(9)).mkString("\t")
      }
      val e = intercept[DataQualityException](land(cfg, events(noValueLines, hdr = noValueHeader)))
      assert(e.getMessage.contains("missing source column(s): value"))
      assert(!DeltaIO.isDeltaTable(spark, s"${cfg.paths.bronzeRoot}/num"))
    }
  }
}
