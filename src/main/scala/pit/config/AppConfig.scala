package pit.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import pureconfig._
import pureconfig.generic.auto._

final case class SparkCfg(appName: String, master: Option[String])
final case class PathsCfg(
    sourceDir: String,
    bronzeRoot: String,
    silverRoot: String,
    goldRoot: String,
    quarantineRoot: String,
    registryPath: String
)
final case class IngestCfg(quarters: List[String])
final case class AppConfig(spark: SparkCfg, paths: PathsCfg, ingest: IngestCfg)

object AppConfig {
  def load(): AppConfig = load(ConfigFactory.load(), sys.env.get("PIT_QUARTERS"))

  /** JAR-task contract: serverless jobs cannot set environment variables, so entrypoints accept the same
    * PIT_* names as `KEY=VALUE` args and load them as system properties, which HOCON substitution resolves
    * exactly like env vars (args win over env).
    */
  def loadFromArgs(args: Array[String]): AppConfig = {
    args.filter(_.contains('=')).foreach { kv =>
      val Array(k, v) = kv.split("=", 2)
      sys.props(k) = v
    }
    ConfigFactory.invalidateCaches()
    load(ConfigFactory.load(), sys.props.get("PIT_QUARTERS").orElse(sys.env.get("PIT_QUARTERS")))
  }

  /** `quartersOverride` is a comma-separated list (`"2026q1,2026q2"`), the PIT_QUARTERS env contract. It
    * cannot ride HOCON's `${?PIT_QUARTERS}` substitution: that injects a STRING where the schema needs a
    * list, and config load would fail.
    */
  def load(config: Config, quartersOverride: Option[String] = None): AppConfig = {
    val base = ConfigSource.fromConfig(config.getConfig("pit")).loadOrThrow[AppConfig]
    quartersOverride
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toList)
      .filter(_.nonEmpty)
      .fold(base)(qs => base.copy(ingest = IngestCfg(qs)))
  }
}
