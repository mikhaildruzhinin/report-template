package dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseDataframe(spark: SparkSession) {
  protected val df: DataFrame = getDF
  def getDF: DataFrame
}
