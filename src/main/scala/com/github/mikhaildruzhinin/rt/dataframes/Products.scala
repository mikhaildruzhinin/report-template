package com.github.mikhaildruzhinin.rt.dataframes

import com.github.mikhaildruzhinin.rt.util.DataFrameLoader
import org.apache.spark.sql.{DataFrame, SparkSession}

class Products(val df: DataFrame) extends BaseDataFrame {
  def this(spark: SparkSession, pathToProductsFile: String) = this(
    Products.getDF(spark, pathToProductsFile)
  )
}

object Products {
  def getDF(spark: SparkSession, pathToProductsFile: String): DataFrame  = {
    val options = Map(
      "inferSchema" -> "true",
      "header" -> "true",
      "sep" -> ",",
      "path" -> pathToProductsFile
    )
    val loader = new DataFrameLoader
    val df = loader.load(spark, "csv", options)
    df.withColumnRenamed("product_name_hash", "name_hash")
  }
}
