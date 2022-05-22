package dataframes

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Products(val df: DataFrame) extends BaseDataFrame {
  def this(spark: SparkSession, pathToProductsFile: String) = this(
    Products.getDF(spark, pathToProductsFile)
  )
}

object Products {
  def getDF(spark: SparkSession, pathToProductsFile: String): DataFrame  = {
    val productSchema = StructType(
      Array(
        StructField("brand", StringType),
        StructField("product_name_hash", StringType)
      )
    )

    spark.read
      .schema(productSchema)
      .option("header", "true")
      .option("sep", ",")
      .csv(pathToProductsFile)
      .withColumnRenamed("product_name_hash", "name_hash")
  }
}
