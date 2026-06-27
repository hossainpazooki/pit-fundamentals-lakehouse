package pit.common

import org.scalatest.funsuite.AnyFunSuite
import pit.batch.BatchManifest

class HashingSpec extends AnyFunSuite {
  test("sha256Hex is deterministic and changes with one byte") {
    val a = Hashing.sha256Hex(Array[Byte](1, 2, 3))
    val b = Hashing.sha256Hex(Array[Byte](1, 2, 4))
    assert(a == Hashing.sha256Hex(Array[Byte](1, 2, 3)))
    assert(a != b)
    assert(a.matches("[0-9a-f]{64}"))
  }

  test("canonicalJson is key-order independent") {
    val x = Hashing.canonicalJson(Seq("b" -> "2", "a" -> "1"))
    val y = Hashing.canonicalJson(Seq("a" -> "1", "b" -> "2"))
    assert(x == y)
  }

  test("batchId commits to source bytes AND code sha (decision, not just input)") {
    val base = BatchManifest(Seq("aaa"), "schemaV1", "codeA", Map("q" -> "2023q1"))
    val diffSource = base.copy(sourceSha256 = Seq("bbb"))
    val diffCode = base.copy(codeSha = "codeB")
    assert(base.batchId != diffSource.batchId, "source byte change must change id")
    assert(base.batchId != diffCode.batchId, "code change must change id")
    assert(base.batchId == base.copy().batchId, "same manifest -> same id")
  }
}
