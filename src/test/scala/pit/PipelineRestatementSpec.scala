package pit

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.row_number
import org.scalatest.funsuite.AnyFunSuite
import pit.config.AppConfig
import pit.config.IngestCfg
import pit.config.PathsCfg
import pit.config.SparkCfg
import pit.gold.GoldPit
import pit.support.Fixtures
import pit.support.Fixtures.Filing
import pit.support.Fixtures.bd
import pit.support.SparkTestSupport

import java.sql.Timestamp

/** Same-key restatement through the PRODUCTION path: `Pipeline.runFromFrames` -> `SilverMerge.upsert` ->
  * `GoldPit.buildGold`, queried via `GoldPit.asOf`. Never a hand-built gold frame — `PitNoLookaheadSpec`
  * proves the gold transform in isolation; THIS spec proves the seam upstream of it, where batches arrive
  * sequentially and the `_ingest_ts` tiebreak lives.
  *
  * The adversarial shape is a same-key restatement arriving OUT OF ORDER: the correcting filing (later
  * `accepted`) is ingested FIRST, so its `_ingest_ts` is EARLIER than the original's. If any `as_of(D)`
  * answer depends on arrival order, load order leaks into the PIT boundary.
  */
class PipelineRestatementSpec extends AnyFunSuite with SparkTestSupport {

  // Same period (cik=1, tag=Assets, ddate=20230331, qtrs=1). A genuine SEC correction is a NEW
  // filing: distinct adsh, later accepted. `ingest` is set per lake to control arrival order.
  private val T1 = "2023-05-01 12:00:00" // original accepted
  private val T2 = "2023-08-01 12:00:00" // correction accepted (T2 > T1)
  private val original = Filing("a1", 1, "Assets", 20230331, 1, bd("100"), T1, "2023-05-02 00:00:00")
  private val correction = Filing("a2", 1, "Assets", 20230331, 1, bd("150"), T2, "2023-08-02 00:00:00")

  private def cfgFor(root: String): AppConfig =
    AppConfig(
      SparkCfg("t", Some("local[2]")),
      PathsCfg(
        s"$root/src",
        s"$root/bronze",
        s"$root/silver",
        s"$root/gold",
        s"$root/quarantine",
        s"$root/registry"
      ),
      IngestCfg(List("t"))
    )

  /** One production batch: a single filing through the full frame-driven pipeline stage path. */
  private def ingestBatch(cfg: AppConfig, batchId: String, fs: Seq[Filing]): Unit = {
    Pipeline.runFromFrames(
      spark,
      cfg,
      num = Fixtures.numDf(spark, fs),
      sub = Fixtures.subDf(spark, fs),
      batchId = batchId,
      ingestTs = Timestamp.valueOf(fs.head.ingest)
    )
    ()
  }

  private def asOfValue(cfg: AppConfig, d: String): Option[Double] = {
    val gold = spark.read.format("delta").load(cfg.paths.goldRoot)
    val r = GoldPit.asOf(gold, Timestamp.valueOf(d)).collect()
    r.headOption.map(_.getAs[java.math.BigDecimal]("value").doubleValue())
  }

  test(
    "same-key restatement, out-of-order arrival: every as_of answer is identical across load orders"
  ) {
    withTempDir { rootIn =>
      withTempDir { rootOut =>
        val inOrder = cfgFor(rootIn)
        val outOfOrder = cfgFor(rootOut)

        // In-order lake: arrival follows accepted order.
        ingestBatch(inOrder, "b-orig", Seq(original.copy(ingest = "2023-05-02 00:00:00")))
        ingestBatch(inOrder, "b-corr", Seq(correction.copy(ingest = "2023-08-02 00:00:00")))

        // Out-of-order lake: the correction arrives FIRST; the original arrives later, so the
        // original carries the LATER _ingest_ts while carrying the EARLIER accepted.
        ingestBatch(outOfOrder, "b-corr", Seq(correction.copy(ingest = "2023-08-02 00:00:00")))
        ingestBatch(outOfOrder, "b-orig", Seq(original.copy(ingest = "2023-09-02 00:00:00")))

        val probes = Seq(
          "2023-04-01 00:00:00" -> None, // D < T1: nothing was accepted yet
          "2023-06-01 00:00:00" -> Some(100.0), // T1 <= D < T2: the original, NEVER the correction
          "2023-09-01 00:00:00" -> Some(150.0) // D >= T2: the correction
        )
        probes.foreach { case (d, expected) =>
          assert(asOfValue(inOrder, d) == expected, s"in-order lake: as_of($d)")
          assert(asOfValue(outOfOrder, d) == expected, s"out-of-order lake: as_of($d)")
        }
      }
    }
  }

  test("validity intervals order strictly by accepted, not by arrival, in both load orders") {
    withTempDir { rootIn =>
      withTempDir { rootOut =>
        val inOrder = cfgFor(rootIn)
        val outOfOrder = cfgFor(rootOut)

        ingestBatch(inOrder, "b-orig", Seq(original.copy(ingest = "2023-05-02 00:00:00")))
        ingestBatch(inOrder, "b-corr", Seq(correction.copy(ingest = "2023-08-02 00:00:00")))
        ingestBatch(outOfOrder, "b-corr", Seq(correction.copy(ingest = "2023-08-02 00:00:00")))
        ingestBatch(outOfOrder, "b-orig", Seq(original.copy(ingest = "2023-09-02 00:00:00")))

        Seq(inOrder, outOfOrder).foreach { cfg =>
          val gold = spark.read.format("delta").load(cfg.paths.goldRoot)
          val rows = gold
            .orderBy(col("accepted"))
            .select("value", "valid_from", "valid_to")
            .collect()
          assert(rows.length == 2, s"expected both filings in gold, got ${rows.length}")
          assert(rows(0).getAs[java.math.BigDecimal]("value").doubleValue() == 100.0)
          assert(rows(0).getAs[Timestamp]("valid_to") == Timestamp.valueOf(T2))
          assert(rows(1).getAs[java.math.BigDecimal]("value").doubleValue() == 150.0)
          assert(rows(1).getAs[Timestamp]("valid_to") == GoldPit.MAX_SENTINEL)
        }
      }
    }
  }

  test(
    "same-adsh resend: silver resolves by the explicit _ingest_ts tiebreak, not arrival order"
  ) {
    // The only shape that collides on silver's 6-col key is a resend of the SAME filing (same
    // adsh, e.g. a re-published quarter). `accepted` is unchanged by a resend, so this tiebreak
    // is a re-publish contract — NOT a lookahead vector; max _ingest_ts wins in any arrival order.
    val v0 = Filing("b1", 2, "Assets", 20230331, 1, bd("100"), T1, "2023-05-02 00:00:00")
    val v1 = Filing("b1", 2, "Assets", 20230331, 1, bd("110"), T1, "2023-06-02 00:00:00")
    withTempDir { rootFwd =>
      withTempDir { rootRev =>
        val fwd = cfgFor(rootFwd)
        val rev = cfgFor(rootRev)
        ingestBatch(fwd, "b-v0", Seq(v0))
        ingestBatch(fwd, "b-v1", Seq(v1))
        ingestBatch(rev, "b-v1", Seq(v1))
        ingestBatch(rev, "b-v0", Seq(v0)) // arrives later but carries the EARLIER _ingest_ts

        Seq(fwd, rev).foreach { cfg =>
          val silver = spark.read.format("delta").load(cfg.paths.silverRoot)
          assert(silver.count() == 1L)
          assert(silver.select(col("value")).head().getDecimal(0).doubleValue() == 110.0)
          assert(asOfValue(cfg, "2023-09-01 00:00:00").contains(110.0))
        }
      }
    }
  }

  test("accepted tie: resolved by the documented deterministic tiebreak in every load order") {
    // Two distinct filings, same natural key, IDENTICAL accepted. `accepted` cannot break the
    // tie, so ordering must fall through to a total, DATA-DERIVED tiebreak — (adsh, version),
    // which silver's merge key guarantees is unique within a gold partition — never to arrival
    // order or physical row order. Greatest adsh wins here, in both load orders.
    val first = Filing("c1", 3, "Assets", 20230331, 1, bd("100"), T1, "2023-05-02 00:00:00")
    val second = Filing("c2", 3, "Assets", 20230331, 1, bd("200"), T1, "2023-05-03 00:00:00")
    withTempDir { rootFwd =>
      withTempDir { rootRev =>
        val fwd = cfgFor(rootFwd)
        val rev = cfgFor(rootRev)
        ingestBatch(fwd, "b-c1", Seq(first))
        ingestBatch(fwd, "b-c2", Seq(second))
        ingestBatch(rev, "b-c2", Seq(second.copy(ingest = "2023-05-02 00:00:00")))
        ingestBatch(rev, "b-c1", Seq(first.copy(ingest = "2023-05-03 00:00:00")))

        val d = "2023-09-01 00:00:00"
        assert(asOfValue(fwd, d).contains(200.0), s"fwd lake: as_of($d) must resolve the tie to max adsh")
        assert(asOfValue(rev, d).contains(200.0), s"rev lake: as_of($d) must resolve the tie to max adsh")
      }
    }
  }

  test("negative control (non-vacuity): an ingest-ordered seam WOULD leak on this fixture") {
    // Proof the fixture can discriminate: recompute as_of with the exact mutation the production
    // ordering refuses — `_ingest_ts` in place of the accepted-led total order — and assert it
    // returns the WRONG (stale) value on the out-of-order lake. If this ever returns the correct
    // value, the fixture has lost its power to detect the leak and the suite must go red.
    withTempDir { root =>
      val cfg = cfgFor(root)
      ingestBatch(cfg, "b-corr", Seq(correction.copy(ingest = "2023-08-02 00:00:00")))
      ingestBatch(cfg, "b-orig", Seq(original.copy(ingest = "2023-09-02 00:00:00")))

      val silver = spark.read.format("delta").load(cfg.paths.silverRoot)
      val subStore = spark.read.format("delta").load(pit.silver.SilverSubStore.path(cfg))
      val joined = silver
        .as("n")
        .join(subStore.as("s"), col("n.adsh") === col("s.adsh"), "inner")
        .select(
          col("s.cik").as("cik"),
          col("n.tag").as("tag"),
          col("n.ddate").as("ddate"),
          col("n.qtrs").as("qtrs"),
          col("n.uom").as("uom"),
          col("n.value").as("value"),
          col("s.accepted").as("accepted"),
          col("n._ingest_ts").as("_ingest_ts")
        )
      val byIngestDesc = Window
        .partitionBy(col("cik"), col("tag"), col("ddate"), col("qtrs"), col("uom"))
        .orderBy(col("_ingest_ts").desc)
      val broken = joined
        .filter(col("accepted") <= lit(Timestamp.valueOf("2023-09-01 00:00:00")))
        .withColumn("_rn", row_number().over(byIngestDesc))
        .filter(col("_rn") === 1)
      val v = broken.select("value").head().getDecimal(0).doubleValue()
      // D >= T2: the CORRECT answer is 150.0 (the correction). The ingest-ordered seam picks the
      // original, which arrived LAST in this lake, and returns the stale 100.0.
      assert(v == 100.0, s"ingest-ordered seam returned $v — fixture no longer discriminates the leak")
    }
  }
}
