package pit.support

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

object Fixtures {
  final case class Filing(
      adsh: String, cik: Long, tag: String, ddate: Int, qtrs: Int,
      value: java.math.BigDecimal, accepted: String, ingest: String)

  def numDf(spark: SparkSession, fs: Seq[Filing]): DataFrame = {
    import spark.implicits._
    fs.map(f => (f.adsh, f.tag, "v1", f.ddate, f.qtrs, f.value, Timestamp.valueOf(f.ingest)))
      .toDF("adsh", "tag", "version", "ddate", "qtrs", "value", "_ingest_ts")
  }

  def subDf(spark: SparkSession, fs: Seq[Filing]): DataFrame = {
    import spark.implicits._
    fs.map(f => (f.adsh, f.cik, Timestamp.valueOf(f.accepted)))
      .toDF("adsh", "cik", "accepted")
  }

  def bd(s: String): java.math.BigDecimal = new java.math.BigDecimal(s)
}
