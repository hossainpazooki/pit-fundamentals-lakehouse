package pit.silver

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType
import org.scalatest.funsuite.AnyFunSuite
import pit.support.SparkTestSupport

class DataQualityGateSpec extends AnyFunSuite with SparkTestSupport {

  private def numDf(rows: Seq[(String, String, String, Int, Int, String, java.math.BigDecimal)]) = {
    import spark.implicits._
    // Cast to the declared silver type (Decimal(28,4)); production feeds the gate
    // cast silver data, so the fixture must match or the strict type precondition
    // correctly flags it Unevaluable.
    rows
      .toDF("adsh", "tag", "version", "ddate", "qtrs", "uom", "value")
      .withColumn("value", col("value").cast(DecimalType(28, 4)))
  }

  test("missing value column -> Unevaluable (the bug this gate exists to catch)") {
    import spark.implicits._
    val df = Seq(("0001", "Assets")).toDF("adsh", "tag")
    DataQualityGate.gate(df, DataQualityGate.silverNumContract, DataQualityGate.requiredNumCols) match {
      case Unevaluable(reason) => assert(reason.toLowerCase.contains("value"))
      case other => fail(s"expected Unevaluable, got $other")
    }
  }

  test("empty batch where rows expected -> Unevaluable, not vacuous Pass") {
    val empty = numDf(Seq.empty)
    assert(
      DataQualityGate
        .gate(empty, DataQualityGate.silverNumContract, DataQualityGate.requiredNumCols)
        .isInstanceOf[Unevaluable]
    )
  }

  test("duplicate natural key -> Fail") {
    val v = new java.math.BigDecimal("100.0000")
    val df = numDf(
      Seq(
        ("0001", "Assets", "v1", 20230331, 1, "USD", v),
        ("0001", "Assets", "v1", 20230331, 1, "USD", v) // dup of (adsh,tag,version,ddate,qtrs,uom)
      )
    )
    assert(
      DataQualityGate
        .gate(df, DataQualityGate.silverNumContract, DataQualityGate.requiredNumCols)
        .isInstanceOf[Fail]
    )
  }

  test("same fact in two units is NOT a duplicate: uom is part of the natural key") {
    val df = numDf(
      Seq(
        ("0001", "Assets", "v1", 20230331, 1, "USD", new java.math.BigDecimal("100.0000")),
        ("0001", "Assets", "v1", 20230331, 1, "EUR", new java.math.BigDecimal("92.0000"))
      )
    )
    assert(
      DataQualityGate.gate(df, DataQualityGate.silverNumContract, DataQualityGate.requiredNumCols) == Pass
    )
  }

  test("clean batch -> Pass") {
    val df = numDf(
      Seq(
        ("0001", "Assets", "v1", 20230331, 1, "USD", new java.math.BigDecimal("100.0000")),
        ("0001", "Liabilities", "v1", 20230331, 1, "USD", new java.math.BigDecimal("40.0000"))
      )
    )
    assert(
      DataQualityGate.gate(df, DataQualityGate.silverNumContract, DataQualityGate.requiredNumCols) == Pass
    )
  }
}
