package pit.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/** Path-based Delta operations expressed through SQL, so ONE code path serves both the classic runtime (local
  * dev + the test suite) and Spark Connect (serverless jobs), where the classic `io.delta.tables` JVM API is
  * unavailable. The 2.12 suite exercises these SQL forms against real local Delta tables -- that is the
  * parity gate for the serverless profile.
  */
object DeltaIO {

  /** Existence probe: resolving the read schema fails on a missing or non-Delta path. */
  def isDeltaTable(spark: SparkSession, path: String): Boolean =
    try {
      spark.read.format("delta").load(path).schema
      true
    } catch {
      case _: Exception => false
    }

  /** Latest Delta version of the table at `path` (retrieval coordinate, not identity). */
  def latestVersion(spark: SparkSession, path: String): Long =
    spark
      .sql(s"DESCRIBE HISTORY delta.`$path` LIMIT 1")
      .select("version")
      .head()
      .getLong(0)

  /** MERGE INTO the Delta table at `path` from `source`.
    *
    * @param matchedUpdateCondition
    *   guard on the UPDATE branch (e.g. the `_ingest_ts` tiebreak); None updates every match.
    * @param updateWhenMatched
    *   false = no UPDATE branch at all (bronze append-only dedupe: matched rows are no-ops).
    */
  def merge(
      spark: SparkSession,
      path: String,
      source: DataFrame,
      onCondition: String,
      matchedUpdateCondition: Option[String] = None,
      updateWhenMatched: Boolean = true
  ): Unit = {
    val view = s"merge_src_${java.util.UUID.randomUUID().toString.replace("-", "")}"
    source.createOrReplaceTempView(view)
    try {
      val updateClause =
        if (!updateWhenMatched) ""
        else
          matchedUpdateCondition.fold("WHEN MATCHED THEN UPDATE SET *")(c =>
            s"WHEN MATCHED AND $c THEN UPDATE SET *"
          )
      spark.sql(
        s"""MERGE INTO delta.`$path` t
           |USING $view s
           |ON $onCondition
           |$updateClause
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin
      )
      ()
    } finally {
      spark.catalog.dropTempView(view)
      ()
    }
  }
}
