package pit.silver

import org.apache.spark.sql.types.DecimalType
import org.scalatest.funsuite.AnyFunSuite
import pit.support.SparkTestSupport

class SilverTransformsSpec extends AnyFunSuite with SparkTestSupport {
  test("castNum produces the declared typed value column") {
    import spark.implicits._
    val bronze = Seq(("0001", "Assets", "v1", "20230331", "1", "USD", "100.5"))
      .toDF("adsh", "tag", "version", "ddate", "qtrs", "uom", "value")
    val out = SilverTransforms.castNum(bronze)
    assert(out.schema("value").dataType.isInstanceOf[DecimalType])
    assert(out.schema("ddate").dataType.typeName == "integer")
    assert(out.head().getAs[java.math.BigDecimal]("value").doubleValue() == 100.5)
  }

  test("scopeEntityLevel keeps only rows with empty/null segments AND coreg, never drops") {
    import spark.implicits._
    val df = Seq(
      ("a1", "", ""),
      ("a2", null.asInstanceOf[String], null.asInstanceOf[String]),
      ("a3", "Segment=US", ""),
      ("a4", "", "SubCo")
    ).toDF("adsh", "segments", "coreg")
    val (entity, scopedOut) = SilverTransforms.scopeEntityLevel(df)
    assert(entity.count() == 2L)
    assert(scopedOut.count() == 2L)
    assert(entity.count() + scopedOut.count() == df.count(), "no row silently dropped")
  }

  test("splitValidQuarantine routes null-key rows to quarantine, never drops") {
    import spark.implicits._
    val df = Seq(
      ("0001", "Assets", java.lang.Integer.valueOf(20230331)),
      (null.asInstanceOf[String], "Bad", java.lang.Integer.valueOf(20230331))
    ).toDF("adsh", "tag", "ddate")
    val (valid, quarantine) = SilverTransforms.splitValidQuarantine(df, Seq("adsh", "tag"))
    assert(valid.count() == 1L)
    assert(quarantine.count() == 1L)
    assert(valid.count() + quarantine.count() == df.count(), "no row silently dropped")
  }
}
