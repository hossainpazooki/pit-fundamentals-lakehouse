package pit.support

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import java.nio.file.Files

trait SparkTestSupport extends BeforeAndAfterAll { self: Suite =>
  // Backing field is mutable for lifecycle; `spark` is a stable identifier so
  // `import spark.implicits._` (which requires a stable path) resolves in specs.
  @transient private var _spark: SparkSession = _
  @transient protected lazy val spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession
      .builder()
      .appName("pit-test")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()
    _spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (_spark != null) _spark.stop()
    super.afterAll()
  }

  protected def withTempDir(f: String => Unit): Unit = {
    val dir = Files.createTempDirectory("pit-test").toFile
    try f(dir.getAbsolutePath)
    finally {
      def del(d: java.io.File): Unit = {
        if (d.isDirectory) Option(d.listFiles()).foreach(_.foreach(del))
        d.delete()
        ()
      }
      del(dir)
    }
  }
}
