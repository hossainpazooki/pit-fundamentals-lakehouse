package pit.batch

import pit.common.Hashing

/** Content-addressed identity. The hash commits to the input AND the decision that produced the output:
  * source bytes, schema version, code SHA, params. Source bytes alone are insufficient -- two code versions
  * over one ZIP would collide and the id would not identify the decision.
  */
final case class BatchManifest(
    sourceSha256: Seq[String],
    schemaVersion: String,
    codeSha: String,
    ingestParams: Map[String, String]
) {
  def toCanonicalJson: String = {
    // Flatten every field into the sorted canonical object so the id is stable.
    val flat: Seq[(String, String)] =
      Seq(
        "source_sha256" -> sourceSha256.sorted.mkString(","),
        "schema_version" -> schemaVersion,
        "code_sha" -> codeSha
      ) ++ ingestParams.toSeq.map { case (k, v) => s"param.$k" -> v }
    Hashing.canonicalJson(flat)
  }

  def batchId: String = Hashing.sha256Hex(toCanonicalJson)
}
