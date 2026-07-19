package pit.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

sealed trait GateOutcome
case object Pass extends GateOutcome
final case class Fail(detail: DataFrame) extends GateOutcome
final case class Unevaluable(reason: String) extends GateOutcome

/** Declarative DQ contract evaluated with plain DataFrame aggregations. Replaced the Deequ suite 2026-07-18
  * for the serverless port: Deequ is classic-Spark-internals-only (and Scala-2.12-only), while these
  * aggregations run identically on the classic runtime and Spark Connect. The pre-existing gate tests pin the
  * outcome semantics across the swap.
  */
final case class GateContract(
    name: String,
    completeCols: Seq[String],
    uniquenessKey: Seq[String],
    nonNegativeCols: Seq[String]
)

object DataQualityGate {

  val requiredNumCols: Seq[(String, DataType)] = {
    import org.apache.spark.sql.types._
    Seq(
      "adsh" -> StringType,
      "tag" -> StringType,
      "uom" -> StringType,
      "value" -> DecimalType(28, 4)
    )
  }

  // Source columns the entity-level scope filter needs; without them scope cannot be
  // established, which is Unevaluable, not assumable. Name-presence only (bronze is
  // all-String pre-cast).
  val scopeCols: Seq[(String, DataType)] = {
    import org.apache.spark.sql.types._
    Seq("segments" -> StringType, "coreg" -> StringType)
  }

  // Silver contract (§6), calibrated on real SEC data (2026q1, GATE-A record): within the
  // entity-level scope the natural key includes `uom` — without it 66% of a real quarter
  // collides; with it, zero collisions measured.
  val silverNumContract: GateContract = GateContract(
    name = "silver-num",
    completeCols = Seq("adsh", "tag", "uom", "value"),
    uniquenessKey = Seq("adsh", "tag", "version", "ddate", "qtrs", "uom"),
    nonNegativeCols = Seq("qtrs")
  )

  private val detailSchema = StructType(
    Seq(
      StructField("check", StringType, nullable = false),
      StructField("constraint", StringType, nullable = false),
      StructField("target", StringType, nullable = false),
      StructField("violations", LongType, nullable = false)
    )
  )

  /** Fail-closed on unevaluable. Preconditions are checked BEFORE the aggregations, because an aggregation
    * over a missing column or an empty frame would otherwise surface as a confusing analyzer error or a
    * vacuous pass -- missing != incomplete (§6).
    */
  def gate(batch: DataFrame, contract: GateContract, required: Seq[(String, DataType)]): GateOutcome = {
    val schema = batch.schema
    val missing = required.flatMap {
      case (c, _) if !schema.fieldNames.contains(c) => Some(s"missing column $c")
      case (c, t) if schema(c).dataType != t =>
        Some(s"column $c is ${schema(c).dataType.simpleString}, expected ${t.simpleString}")
      case _ => None
    }
    if (missing.nonEmpty) return Unevaluable(missing.mkString("; "))
    // limit(1).count instead of isEmpty: identical semantics, portable to Spark Connect.
    if (batch.limit(1).count() == 0L) return Unevaluable("empty batch where rows expected")

    try {
      val violations = Seq.newBuilder[(String, String, Long)]
      contract.completeCols.foreach { c =>
        val nulls = batch.filter(col(c).isNull).count()
        if (nulls > 0) violations += (("completeness", c, nulls))
      }
      if (contract.uniquenessKey.nonEmpty) {
        val dupGroups = batch
          .groupBy(contract.uniquenessKey.map(col): _*)
          .count()
          .filter(col("count") > 1)
          .count()
        if (dupGroups > 0)
          violations += (("uniqueness", contract.uniquenessKey.mkString(","), dupGroups))
      }
      contract.nonNegativeCols.foreach { c =>
        val negatives = batch.filter(col(c) < 0).count()
        if (negatives > 0) violations += (("non-negative", c, negatives))
      }
      val found = violations.result()
      if (found.isEmpty) Pass
      else {
        val rows = new java.util.ArrayList[Row]()
        found.foreach { case (kind, target, n) => rows.add(Row(contract.name, kind, target, n)) }
        Fail(batch.sparkSession.createDataFrame(rows, detailSchema))
      }
    } catch {
      case e: Throwable => Unevaluable(s"analyzer error: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }
  }

  /** Source-level precondition (name presence only — bronze is all-String pre-cast). Run BEFORE castNum,
    * which manufactures absent declared columns as typed nulls and would otherwise mask a missing required
    * column as a completeness Fail rather than Unevaluable. Preserves §6's missing != incomplete distinction
    * in the shipping path.
    */
  def requiredColumnsPresent(source: DataFrame, required: Seq[(String, DataType)]): Option[String] = {
    val missing = required.map(_._1).filterNot(source.columns.contains)
    if (missing.isEmpty) None else Some("missing source column(s): " + missing.mkString(", "))
  }
}
