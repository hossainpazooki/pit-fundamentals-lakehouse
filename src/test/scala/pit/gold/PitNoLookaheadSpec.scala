package pit.gold

import org.scalatest.funsuite.AnyFunSuite
import pit.support.Fixtures
import pit.support.Fixtures.Filing
import pit.support.Fixtures.bd
import pit.support.SparkTestSupport

import java.sql.Timestamp

class PitNoLookaheadSpec extends AnyFunSuite with SparkTestSupport {

  // Same period (cik=1, tag=Assets, ddate=20230331, qtrs=1), two filings.
  private val T1 = "2023-05-01 12:00:00" // original accepted
  private val T2 = "2023-08-01 12:00:00" // restatement accepted (T2 > T1)
  private val original = Filing("a1", 1, "Assets", 20230331, 1, bd("100"), T1, "2023-05-02 00:00:00")
  private val restated = Filing("a2", 1, "Assets", 20230331, 1, bd("150"), T2, "2023-08-02 00:00:00")

  private def gold(filings: Seq[Filing]) =
    GoldPit.buildGold(Fixtures.numDf(spark, filings), Fixtures.subDf(spark, filings))

  private def asOfValue(filings: Seq[Filing], d: String): Option[Double] = {
    val r = GoldPit.asOf(gold(filings), Timestamp.valueOf(d)).collect()
    r.headOption.map(_.getAs[java.math.BigDecimal]("value").doubleValue())
  }

  test("D < T1 returns nothing") {
    assert(asOfValue(Seq(original, restated), "2023-04-01 00:00:00").isEmpty)
  }

  test("T1 <= D < T2 returns the ORIGINAL value, never the restated one (the leak)") {
    assert(asOfValue(Seq(original, restated), "2023-06-01 00:00:00").contains(100.0))
  }

  test("D >= T2 returns the restated value") {
    assert(asOfValue(Seq(original, restated), "2023-09-01 00:00:00").contains(150.0))
  }

  test("permutation invariance: ingest order cannot change a PIT answer") {
    val fwd = Seq(original, restated)
    val rev = Seq(restated, original)
    Seq("2023-04-01 00:00:00", "2023-06-01 00:00:00", "2023-09-01 00:00:00").foreach { d =>
      assert(asOfValue(fwd, d) == asOfValue(rev, d), s"as_of($d) differs by load order")
    }
  }

  test("uom is part of the natural key: two units of one fact do not collapse or restate each other") {
    val usd = Filing("a1", 1, "Assets", 20230331, 1, bd("100"), T1, "2023-05-02 00:00:00")
    val eur = Filing("a2", 1, "Assets", 20230331, 1, bd("92"), T2, "2023-08-02 00:00:00", uom = "EUR")
    // If uom were absent from the key, the EUR filing would read as a restatement of the USD
    // one and as_of(D >= T2) would return a single (wrong) row.
    val out = GoldPit.asOf(gold(Seq(usd, eur)), Timestamp.valueOf("2023-09-01 00:00:00")).collect()
    assert(out.length == 2, s"expected both units to survive, got ${out.length}")
  }
}
