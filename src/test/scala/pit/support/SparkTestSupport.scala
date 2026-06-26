package pit.support

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkTestSupport extends BeforeAndAfterAll { self: Suite =>
  @transient protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
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
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
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
