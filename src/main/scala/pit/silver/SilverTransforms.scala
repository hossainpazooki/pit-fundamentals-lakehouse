package pit.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import pit.common.Schemas

object SilverTransforms {

  /** Cast declared num columns to the typed schema. Keep load-provenance cols. */
  def castNum(bronze: DataFrame): DataFrame = {
    val casted = Schemas.numTyped.fields.foldLeft(bronze) { (df, f) =>
      if (df.columns.contains(f.name)) df.withColumn(f.name, col(f.name).cast(f.dataType))
      else df.withColumn(f.name, lit(null).cast(f.dataType))
    }
    val keep = Schemas.numColumns ++ Seq("_ingest_ts", "_batch_id").filter(casted.columns.contains)
    casted.select(keep.distinct.map(col): _*)
  }

  /** Structurally invalid rows (null required keys) -> quarantine. Pure split. */
  def splitValidQuarantine(df: DataFrame, required: Seq[String]): (DataFrame, DataFrame) = {
    val validPred = required.map(c => col(c).isNotNull).reduce(_ && _)
    (df.filter(validPred), df.filter(!validPred))
  }
}
