package pit.ingest

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import pit.batch.BatchManifest
import pit.batch.BatchRecord
import pit.batch.BatchRegistry
import pit.bronze.BronzeIngest
import pit.common.Hashing
import pit.common.Schemas
import pit.config.AppConfig

import java.nio.file.Files
import java.nio.file.Paths

/** Frames returned by the real TSV read path. `num` is bronze-shaped (declared string cols plus audit
  * columns); `sub` is the typed subset Pipeline.runFromFrames joins gold on.
  */
final case class IngestResult(num: DataFrame, sub: DataFrame, batchId: String, bronzeNumVersion: Long)

object TsvIngest {

  /** Ingest one quarter dir of SEC TSVs into bronze, register the batch, and return frames for
    * Pipeline.runFromFrames. Reads `${cfg.paths.sourceDir}/$quarter/num.txt` and `.../sub.txt`. Reuses
    * BronzeIngest, BatchManifest, Hashing, BatchRegistry, Schemas — nothing reimplemented.
    */
  def ingestQuarter(
      spark: SparkSession,
      cfg: AppConfig,
      quarter: String,
      codeSha: String,
      ingestTs: java.sql.Timestamp
  ): IngestResult = {
    val numPath = s"${cfg.paths.sourceDir}/$quarter/num.txt"
    val subPath = s"${cfg.paths.sourceDir}/$quarter/sub.txt"

    // 1. Header-named bronze reads (inference banned), declared columns only.
    val numRaw = BronzeIngest.readTsv(spark, numPath, Schemas.numColumns)
    val subRaw = BronzeIngest.readTsv(spark, subPath, Schemas.subColumns)

    // 2. Content-addressed batch identity over the real source bytes + schema + code + params.
    val manifest = BatchManifest(
      sourceSha256 = Seq(
        Hashing.sha256Hex(Files.readAllBytes(Paths.get(numPath))),
        Hashing.sha256Hex(Files.readAllBytes(Paths.get(subPath)))
      ),
      schemaVersion = Schemas.schemaVersion,
      codeSha = codeSha,
      ingestParams = Map("quarter" -> quarter)
    )
    val batchId = manifest.batchId

    // 3. Audit + append-only bronze (idempotent on (_batch_id,_src_row_hash)).
    val numBronze = BronzeIngest.withAuditColumns(numRaw, batchId, "num.txt", ingestTs)
    BronzeIngest.appendIdempotent(spark, s"${cfg.paths.bronzeRoot}/num", numBronze)
    val subBronze = BronzeIngest.withAuditColumns(subRaw, batchId, "sub.txt", ingestTs)
    BronzeIngest.appendIdempotent(spark, s"${cfg.paths.bronzeRoot}/sub", subBronze)

    // 4. Delta version is RETRIEVAL coordinate, not identity (see CONTRACT §1).
    val bronzeNumVersion = pit.util.DeltaIO.latestVersion(spark, s"${cfg.paths.bronzeRoot}/num")

    // 5. Register the batch so the manifest+version are recoverable by id.
    BatchRegistry.register(
      spark,
      cfg.paths.registryPath,
      BatchRecord(batchId, manifest.toCanonicalJson, bronzeNumVersion)
    )

    // 6. Typed sub projection consumed by GoldPit's adsh join + accepted ordering.
    val typedSub = subRaw.select(
      col("adsh").cast(StringType),
      col("cik").cast(LongType),
      col("accepted").cast(TimestampType)
    )

    // 7. num is the bronze-shaped frame; sub is the typed subset.
    IngestResult(numBronze, typedSub, batchId, bronzeNumVersion)
  }
}
