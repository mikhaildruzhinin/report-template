package com.github.mikhaildruzhinin.rt.dataframes

import com.github.mikhaildruzhinin.rt.util.DataFrameLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KktCategories(val df: DataFrame) extends BaseDataFrame {
  def this(spark: SparkSession, databaseUrl: String) = this(
    KktCategories.getDF(spark, databaseUrl)
  )

  def getKktNumbersDF(categories: String): DataFrame = {
    df
      .filter(col("category").isInCollection(categories.split(",")))
      .withColumn(
        "rn",
        row_number().over(
          Window.partitionBy("kkt_number").orderBy(desc("version"))
        )
      )
      .filter(col("rn") === 1)
      .select("kkt_number")
      .withColumnRenamed("kkt_number", "number")
  }
}

object KktCategories {
  def getDF(spark: SparkSession, databaseUrl: String): DataFrame = {
    val options = Map(
      "url" -> databaseUrl,
      "dbtable" -> "kkt_categories"
    )
    val loader = new DataFrameLoader
    loader.load(spark, "jdbc", options)
  }
}
