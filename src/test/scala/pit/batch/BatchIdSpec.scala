package pit.batch

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import pit.bronze.BronzeIngest
import pit.common.Hashing
import pit.support.SparkTestSupport

import java.sql.Timestamp

class BatchIdSpec extends AnyFunSuite with SparkTestSupport {

  test("mutating one source byte changes the batch id") {
    val src = Array[Byte](10, 20, 30)
    val m1 = BatchManifest(Seq(Hashing.sha256Hex(src)), "schemaV1", "codeA", Map("q" -> "2023q1"))
    val mutated = src.clone(); mutated(1) = 21
    val m2 = m1.copy(sourceSha256 = Seq(Hashing.sha256Hex(mutated)))
    assert(m1.batchId != m2.batchId)
  }

  test("replay against the original hash reproduces the original version's rows") {
    withTempDir { root =>
      val registry = s"$root/registry"
      val bronze = s"$root/bronze"
      val ts = Timestamp.valueOf("2023-04-01 00:00:00")

      import spark.implicits._
      val raw = Seq(("0001", "Assets", "100"), ("0001", "Liabilities", "40"))
        .toDF("adsh", "tag", "value")
      val manifest = BatchManifest(Seq("sha-of-zip"), "schemaV1", "codeA", Map("q" -> "2023q1"))
      val batchId = manifest.batchId

      val batch = BronzeIngest.withAuditColumns(raw, batchId, "num.txt", ts)
      BronzeIngest.appendIdempotent(spark, bronze, batch)
      val version =
        spark.read.format("delta").load(bronze).sparkSession.read.format("delta").load(bronze) // settle
      val deltaVersion =
        io.delta.tables.DeltaTable.forPath(spark, bronze).history(1).head().getAs[Long]("version")

      BatchRegistry.register(spark, registry, BatchRecord(batchId, manifest.toCanonicalJson, deltaVersion))

      // Replay: look up by hash, retrieve the recorded Delta version-as-of.
      val rec = BatchRegistry.lookup(spark, registry, batchId)
      assert(rec.isDefined, "registry must resolve the batch id")
      val replayed = spark.read
        .format("delta")
        .option("versionAsOf", rec.get.deltaVersion)
        .load(bronze)
        .filter(col("_batch_id") === batchId)
      assert(replayed.count() == 2L)
      // Identity is the hash; time-travel is retrieval — both must agree.
      assert(rec.get.batchId == batchId)
    }
  }
}
