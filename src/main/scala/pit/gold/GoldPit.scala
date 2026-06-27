package pit.gold

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.Timestamp

object GoldPit {
  val MAX_SENTINEL: Timestamp = Timestamp.valueOf("9999-12-31 00:00:00")

  /** Validity intervals ordered STRICTLY by `accepted` (the SEC timestamp), never by `_ingest_ts`. Ordering
    * by ingest order would leak lookahead.
    */
  def buildGold(numSilver: DataFrame, sub: DataFrame): DataFrame = {
    val joined = numSilver
      .as("n")
      .join(sub.as("s"), col("n.adsh") === col("s.adsh"), "inner")
      .select(
        col("s.cik").as("cik"),
        col("n.tag").as("tag"),
        col("n.ddate").as("ddate"),
        col("n.qtrs").as("qtrs"),
        col("n.value").as("value"),
        col("s.accepted").as("accepted")
      )

    val byAccepted =
      Window.partitionBy("cik", "tag", "ddate", "qtrs").orderBy(col("accepted").asc)

    joined
      .withColumn("valid_from", col("accepted"))
      .withColumn(
        "valid_to",
        coalesce(lead(col("accepted"), 1).over(byAccepted), lit(MAX_SENTINEL))
      )
  }

  /** "Fundamentals as of D": accepted <= D, latest restatement per natural key. */
  def asOf(gold: DataFrame, d: Timestamp): DataFrame = {
    val byAcceptedDesc =
      Window.partitionBy("cik", "tag", "ddate", "qtrs").orderBy(col("accepted").desc)
    gold
      .filter(col("accepted") <= lit(d))
      .withColumn("_rn", row_number().over(byAcceptedDesc))
      .filter(col("_rn") === 1)
      .drop("_rn")
  }
}
