package util

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameLoader {
  def load(spark: SparkSession, format: String, options: Map[String, String]): DataFrame = {
    spark.read
      .format(format)
      .options(options)
      .load()
  }
}
