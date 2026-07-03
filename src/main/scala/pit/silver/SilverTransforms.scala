package pit.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import pit.common.Schemas

object SilverTransforms {

  /** Entity-level scope (consolidated, no coregistrant): keep rows where `segments` and `coreg` are both
    * empty/null; the rest are segment/coregistrant breakouts, excluded by design and counted by the caller —
    * not quarantined. Pure split, no row silently dropped.
    */
  def scopeEntityLevel(df: DataFrame): (DataFrame, DataFrame) = {
    val entityPred =
      coalesce(col("segments"), lit("")) === "" && coalesce(col("coreg"), lit("")) === ""
    (df.filter(entityPred), df.filter(!entityPred))
  }

  /** Cast declared num columns to the typed schema. Keep load-provenance cols; the scope columns
    * (segments/coreg) are spent by then and dropped from silver.
    */
  def castNum(bronze: DataFrame): DataFrame = {
    val casted = Schemas.numTyped.fields.foldLeft(bronze) { (df, f) =>
      if (df.columns.contains(f.name)) df.withColumn(f.name, col(f.name).cast(f.dataType))
      else df.withColumn(f.name, lit(null).cast(f.dataType))
    }
    val keep =
      Schemas.numTyped.fieldNames.toSeq ++ Seq("_ingest_ts", "_batch_id").filter(casted.columns.contains)
    casted.select(keep.distinct.map(col): _*)
  }

  /** Structurally invalid rows (null required keys) -> quarantine. Pure split. */
  def splitValidQuarantine(df: DataFrame, required: Seq[String]): (DataFrame, DataFrame) = {
    val validPred = required.map(c => col(c).isNotNull).reduce(_ && _)
    (df.filter(validPred), df.filter(!validPred))
  }
}
