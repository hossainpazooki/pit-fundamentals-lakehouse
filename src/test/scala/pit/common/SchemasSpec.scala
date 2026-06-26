package pit.common

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class SchemasSpec extends AnyFunSuite {
  test("fingerprint is stable and order-sensitive") {
    val a = StructType(Seq(StructField("x", StringType), StructField("y", LongType)))
    val b = StructType(Seq(StructField("y", LongType), StructField("x", StringType)))
    assert(Schemas.fingerprint(a) == Schemas.fingerprint(a))
    assert(Schemas.fingerprint(a) != Schemas.fingerprint(b))
  }

  test("declared num columns include the PIT-load-bearing fields") {
    Seq("adsh", "tag", "version", "ddate", "qtrs", "value")
      .foreach(c => assert(Schemas.numColumns.contains(c), s"missing $c"))
  }

  test("schemaVersion is a non-empty hex string") {
    assert(Schemas.schemaVersion.matches("[0-9a-f]{64}"))
  }
}
