package pit.ingest

import io.delta.tables.DeltaTable
import org.apache.spark.sql.types.TimestampType
import org.scalatest.funsuite.AnyFunSuite
import pit.Pipeline
import pit.batch.BatchRegistry
import pit.config.AppConfig
import pit.config.IngestCfg
import pit.config.PathsCfg
import pit.config.SparkCfg
import pit.support.SparkTestSupport

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Timestamp

class TsvIngestSpec extends AnyFunSuite with SparkTestSupport {

  private val ingestTs = Timestamp.valueOf("2023-08-02 00:00:00")
  private val quarter = "2023q2"

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
      IngestCfg(List(quarter))
    )

  /** Writes tiny tab-separated num.txt + sub.txt with header rows matching the declared schemas. */
  private def writeFixtures(root: String): Unit = {
    val q = Paths.get(s"$root/src/$quarter")
    Files.createDirectories(q)
    // Header matches the real SEC num.txt column order; segments/coreg empty = entity-level.
    val num = Seq(
      Seq("adsh", "tag", "version", "ddate", "qtrs", "uom", "segments", "coreg", "value"),
      Seq("a1", "Assets", "v1", "20230331", "1", "USD", "", "", "100"),
      Seq("a2", "Assets", "v1", "20230331", "1", "USD", "", "", "150"),
      Seq("a2", "Assets", "v1", "20230331", "1", "USD", "Segment=US", "", "90")
    ).map(_.mkString("\t")).mkString("\n")
    val sub = Seq(
      Seq("adsh", "cik", "name", "form", "period", "fy", "fp", "filed", "accepted"),
      Seq("a1", "1", "Co", "10-K", "20230331", "2023", "FY", "20230501", "2023-05-02 00:00:00"),
      Seq("a2", "1", "Co", "10-K", "20230331", "2023", "FY", "20230801", "2023-08-02 00:00:00")
    ).map(_.mkString("\t")).mkString("\n")
    Files.write(q.resolve("num.txt"), num.getBytes(StandardCharsets.UTF_8))
    Files.write(q.resolve("sub.txt"), sub.getBytes(StandardCharsets.UTF_8))
    ()
  }

  test("ingestQuarter lands bronze, registers the batch, and types sub.accepted") {
    withTempDir { root =>
      val cfg = cfgFor(root)
      writeFixtures(root)

      val r = TsvIngest.ingestQuarter(spark, cfg, quarter, "code-sha-1", ingestTs)

      // Bronze tables exist and carry the audit columns.
      assert(DeltaTable.isDeltaTable(spark, s"${cfg.paths.bronzeRoot}/num"))
      assert(DeltaTable.isDeltaTable(spark, s"${cfg.paths.bronzeRoot}/sub"))
      assert(r.num.columns.contains("_batch_id"))
      // Bronze keeps ALL source rows, including the segment breakout; scoping happens at silver.
      assert(spark.read.format("delta").load(s"${cfg.paths.bronzeRoot}/num").count() == 3L)

      // Batch is registered and resolvable by id.
      val rec = BatchRegistry.lookup(spark, cfg.paths.registryPath, r.batchId)
      assert(rec.nonEmpty, "batch not registered")
      assert(rec.get.deltaVersion == r.bronzeNumVersion)

      // sub.accepted is a real Timestamp, not a string.
      assert(r.sub.schema("accepted").dataType == TimestampType)
      assert(r.sub.count() == 2L)
    }
  }

  test("re-ingesting the same quarter is idempotent (bronze num count unchanged)") {
    withTempDir { root =>
      val cfg = cfgFor(root)
      writeFixtures(root)

      TsvIngest.ingestQuarter(spark, cfg, quarter, "code-sha-1", ingestTs)
      val first = spark.read.format("delta").load(s"${cfg.paths.bronzeRoot}/num").count()
      val second = TsvIngest.ingestQuarter(spark, cfg, quarter, "code-sha-1", ingestTs)
      val after = spark.read.format("delta").load(s"${cfg.paths.bronzeRoot}/num").count()

      assert(first == 3L)
      assert(after == 3L, s"re-ingest changed bronze count to $after")
      // Same source bytes + schema + code + params -> same content-addressed id.
      assert(second.batchId == TsvIngest.ingestQuarter(spark, cfg, quarter, "code-sha-1", ingestTs).batchId)
    }
  }

  test("the real read path composes through runFromFrames to a non-empty gold") {
    withTempDir { root =>
      val cfg = cfgFor(root)
      writeFixtures(root)

      val r = TsvIngest.ingestQuarter(spark, cfg, quarter, "code-sha-1", ingestTs)
      val m = Pipeline.runFromFrames(spark, cfg, r.num, r.sub, r.batchId, ingestTs)

      assert(m.gateOutcome == "Pass", s"gate was ${m.gateOutcome}")
      assert(m.scopedOut == 1L, s"segment breakout not scoped out: ${m.asLogLine}")
      val gold = spark.read.format("delta").load(cfg.paths.goldRoot)
      assert(gold.count() == 2L, "gold must hold exactly the two entity-level facts")
    }
  }
}
