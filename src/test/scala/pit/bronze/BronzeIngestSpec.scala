package pit.bronze

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import pit.support.SparkTestSupport

import java.sql.Timestamp

class BronzeIngestSpec extends AnyFunSuite with SparkTestSupport {
  test("re-ingesting the same batch is idempotent (no double-count)") {
    withTempDir { dir =>
      val ts = Timestamp.valueOf("2023-04-01 00:00:00")
      import spark.implicits._
      val raw = Seq(("0001", "Assets", "100")).toDF("adsh", "tag", "value")
      val withAudit =
        BronzeIngest.withAuditColumns(raw, batchId = "b1", sourceFile = "num.txt", ingestTs = ts)

      BronzeIngest.appendIdempotent(spark, dir, withAudit)
      BronzeIngest.appendIdempotent(spark, dir, withAudit) // same batch again

      val n = spark.read.format("delta").load(dir).count()
      assert(n == 1L, s"expected 1 row after idempotent re-ingest, got $n")
    }
  }

  test("audit columns are present after withAuditColumns") {
    import spark.implicits._
    val ts = Timestamp.valueOf("2023-04-01 00:00:00")
    val raw = Seq(("0001", "Assets", "100")).toDF("adsh", "tag", "value")
    val out = BronzeIngest.withAuditColumns(raw, "b1", "num.txt", ts)
    Seq("_ingest_ts", "_source_file", "_batch_id", "_src_row_hash")
      .foreach(c => assert(out.columns.contains(c), s"missing audit col $c"))
    assert(out.select(col("_src_row_hash")).head().getString(0).matches("[0-9a-f]{64}"))
  }
}
