package pit.config

import com.typesafe.config.{Config, ConfigFactory}
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
  def load(): AppConfig = load(ConfigFactory.load())

  def load(config: Config): AppConfig =
    ConfigSource.fromConfig(config.getConfig("pit")).loadOrThrow[AppConfig]
}
