package pit.common

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object Hashing {
  def sha256Hex(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"$b%02x").mkString

  def sha256Hex(s: String): String = sha256Hex(s.getBytes(StandardCharsets.UTF_8))

  /** Minimal canonical JSON object: keys sorted, values string-escaped. */
  def canonicalJson(fields: Seq[(String, String)]): String =
    fields
      .sortBy(_._1)
      .map { case (k, v) => s"${quote(k)}:${quote(v)}" }
      .mkString("{", ",", "}")

  private def quote(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
