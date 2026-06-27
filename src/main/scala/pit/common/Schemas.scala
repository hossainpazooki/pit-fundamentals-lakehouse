package pit.common

import org.apache.spark.sql.types._

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object Schemas {
  // Declared bronze column names (subset we commit to), in SEC source order.
  val subColumns: Seq[String] =
    Seq("adsh", "cik", "name", "form", "period", "fy", "fp", "filed", "accepted")

  val numColumns: Seq[String] =
    Seq("adsh", "tag", "version", "ddate", "qtrs", "uom", "value")

  // Declared silver typed schemas (what casting targets).
  val subTyped: StructType = StructType(
    Seq(
      StructField("adsh", StringType, nullable = false),
      StructField("cik", LongType),
      StructField("name", StringType),
      StructField("form", StringType),
      StructField("period", IntegerType),
      StructField("fy", IntegerType),
      StructField("fp", StringType),
      StructField("filed", IntegerType),
      StructField("accepted", TimestampType, nullable = false)
    )
  )

  val numTyped: StructType = StructType(
    Seq(
      StructField("adsh", StringType, nullable = false),
      StructField("tag", StringType, nullable = false),
      StructField("version", StringType, nullable = false),
      StructField("ddate", IntegerType, nullable = false),
      StructField("qtrs", IntegerType, nullable = false),
      StructField("uom", StringType),
      StructField("value", DecimalType(28, 4))
    )
  )

  def fingerprint(schema: StructType): String = {
    // Canonical: name:type:nullable joined, then SHA-256. Order-sensitive by design.
    val canon = schema.fields
      .map(f => s"${f.name}:${f.dataType.simpleString}:${f.nullable}")
      .mkString("|")
    sha256Hex(canon)
  }

  val schemaVersion: String = fingerprint(
    StructType(numTyped.fields.toSeq ++ subTyped.fields.toSeq)
  )

  private def sha256Hex(s: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(s.getBytes(StandardCharsets.UTF_8))
      .map(b => f"$b%02x")
      .mkString
}
