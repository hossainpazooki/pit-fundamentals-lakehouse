package pit.gold

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.Timestamp

object GoldPit {
  val MAX_SENTINEL: Timestamp = Timestamp.valueOf("9999-12-31 00:00:00")

  // Natural key of an entity-level fact: `uom` is part of it (same tag can be reported in
  // more than one unit); collapsing it would chain unrelated values into one interval set.
  private val naturalKey = Seq("cik", "tag", "ddate", "qtrs", "uom")

  // Restatement order within a natural key: STRICTLY `accepted` (the SEC timestamp), never
  // `_ingest_ts` — ingest order would leak lookahead. Ties on `accepted` fall through to the
  // data-derived provenance pair (adsh, version), which silver's merge key guarantees is unique
  // within a partition, so the ordering is TOTAL and load-order independent.
  private val restatementOrder = Seq("accepted", "adsh", "version")

  /** Validity intervals ordered by the total restatement order above. */
  def buildGold(numSilver: DataFrame, sub: DataFrame): DataFrame = {
    val joined = numSilver
      .as("n")
      .join(sub.as("s"), col("n.adsh") === col("s.adsh"), "inner")
      .select(
        col("s.cik").as("cik"),
        col("n.tag").as("tag"),
        col("n.ddate").as("ddate"),
        col("n.qtrs").as("qtrs"),
        col("n.uom").as("uom"),
        col("n.value").as("value"),
        col("s.accepted").as("accepted"),
        // Provenance: which filing said so, and the tie keys that make the ordering total.
        col("n.adsh").as("adsh"),
        col("n.version").as("version")
      )

    val byAccepted =
      Window.partitionBy(naturalKey.map(col): _*).orderBy(restatementOrder.map(c => col(c).asc): _*)

    joined
      .withColumn("valid_from", col("accepted"))
      .withColumn(
        "valid_to",
        coalesce(lead(col("accepted"), 1).over(byAccepted), lit(MAX_SENTINEL))
      )
  }

  /** "Fundamentals as of D": accepted <= D, latest restatement per natural key (by the same total restatement
    * order as `buildGold`, so an `accepted` tie never depends on load order).
    */
  def asOf(gold: DataFrame, d: Timestamp): DataFrame = {
    val byAcceptedDesc =
      Window.partitionBy(naturalKey.map(col): _*).orderBy(restatementOrder.map(c => col(c).desc): _*)
    gold
      .filter(col("accepted") <= lit(d))
      .withColumn("_rn", row_number().over(byAcceptedDesc))
      .filter(col("_rn") === 1)
      .drop("_rn")
  }
}
