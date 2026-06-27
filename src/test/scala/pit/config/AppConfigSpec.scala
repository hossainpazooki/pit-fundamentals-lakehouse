package pit.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class AppConfigSpec extends AnyFunSuite {
  test("load resolves paths and ingest config from a Config") {
    val raw = ConfigFactory.parseString("""pit {
        |  spark { app-name = "t", master = "local[1]" }
        |  paths {
        |    source-dir = "/s", bronze-root = "/b", silver-root = "/sv"
        |    gold-root = "/g", quarantine-root = "/q", registry-path = "/r"
        |  }
        |  ingest { quarters = ["2023q1", "2023q2"] }
        |}""".stripMargin)
    val cfg = AppConfig.load(raw)
    assert(cfg.spark.master.contains("local[1]"))
    assert(cfg.paths.bronzeRoot == "/b")
    assert(cfg.ingest.quarters == List("2023q1", "2023q2"))
  }
}
