package dataframes

import org.apache.spark.sql.functions.{col, round, sum}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Products(spark: SparkSession, pathToProductsFile: String) extends BaseDataframe(spark: SparkSession) {
  override def getDF: DataFrame = {
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
  }

  def aggregateByBrand(sales: Sales): DataFrame = {
    val salesDF = sales.getDF
    val joinCondition = df.col("product_name_hash") === salesDF.col("sales_product_name_hash")
    df.join(
      salesDF,
      joinCondition,
      "inner"
    ).groupBy("brand")
      .agg(
        round(
          sum("total_sum"),
          2
        ).as("total_sum"))
      .orderBy("brand")
      .withColumn(
        "total_sum_pct",
        round(
          col("total_sum") / sum("total_sum").over(),
          2
        )
      )
  }
}
