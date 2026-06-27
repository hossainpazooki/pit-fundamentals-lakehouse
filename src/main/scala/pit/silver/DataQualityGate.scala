package pit.silver

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.Check
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.checks.CheckStatus
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

sealed trait GateOutcome
case object Pass extends GateOutcome
final case class Fail(detail: DataFrame) extends GateOutcome
final case class Unevaluable(reason: String) extends GateOutcome

object DataQualityGate {

  val requiredNumCols: Seq[(String, DataType)] = {
    import org.apache.spark.sql.types._
    Seq(
      "adsh" -> StringType,
      "tag" -> StringType,
      "value" -> DecimalType(28, 4)
    )
  }

  // Illustrative silver contract (§6).
  val silverNumContract: Check =
    Check(CheckLevel.Error, "silver-num")
      .isComplete("adsh")
      .isComplete("tag")
      .isComplete("value")
      .hasUniqueness(Seq("adsh", "tag", "version", "ddate", "qtrs"), _ == 1.0)
      .isNonNegative("qtrs")

  /** Fail-closed on unevaluable. Preconditions are checked BEFORE the suite, because Deequ will otherwise
    * silently pass a check whose target column or rows do not exist -- vacuous success masquerading as
    * verification.
    */
  def gate(batch: DataFrame, contract: Check, required: Seq[(String, DataType)]): GateOutcome = {
    val spark: SparkSession = batch.sparkSession
    val schema = batch.schema
    val missing = required.flatMap {
      case (c, t) if !schema.fieldNames.contains(c) => Some(s"missing column $c")
      case (c, t) if schema(c).dataType != t =>
        Some(s"column $c is ${schema(c).dataType.simpleString}, expected ${t.simpleString}")
      case _ => None
    }
    if (missing.nonEmpty) return Unevaluable(missing.mkString("; "))
    if (batch.isEmpty) return Unevaluable("empty batch where rows expected")

    try {
      val res = VerificationSuite().onData(batch).addCheck(contract).run()
      res.status match {
        case CheckStatus.Success => Pass
        case _ => Fail(VerificationResult.checkResultsAsDataFrame(spark, res))
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
