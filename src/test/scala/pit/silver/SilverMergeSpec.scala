package pit.silver

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import pit.support.SparkTestSupport

import java.sql.Timestamp

class SilverMergeSpec extends AnyFunSuite with SparkTestSupport {
  private def row(value: String, ingest: String) = {
    import spark.implicits._
    Seq(
      ("0001", "Assets", "v1", 20230331, 1, "USD", new java.math.BigDecimal(value), Timestamp.valueOf(ingest))
    ).toDF("adsh", "tag", "version", "ddate", "qtrs", "uom", "value", "_ingest_ts")
  }

  test("latest _ingest_ts wins on the natural key; re-running is a no-op") {
    withTempDir { dir =>
      SilverMerge.upsert(spark, dir, row("100", "2023-04-01 00:00:00"))
      SilverMerge.upsert(spark, dir, row("100", "2023-04-01 00:00:00")) // same -> no-op
      SilverMerge.upsert(spark, dir, row("150", "2023-05-01 00:00:00")) // later -> wins

      val out = spark.read.format("delta").load(dir)
      assert(out.count() == 1L)
      assert(out.select(col("value")).head().getDecimal(0).doubleValue() == 150.0)
    }
  }
}
