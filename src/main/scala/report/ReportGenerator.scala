package report

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round, sum}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ReportGenerator(
  spark: SparkSession,
  pathToProductsFile: String,
  databaseUrl: String,
  dateFrom: String,
  dateTo: String
 ) {
  def generate(): Unit = {
    val productSchema = StructType(
      Array(
        StructField("brand", StringType),
        StructField("product_name_hash", StringType)
      )
    )

    val product = spark.read
      .schema(productSchema)
      .option("header", "true")
      .option("sep", ",")
      .csv(pathToProductsFile)

    val sales = spark.read
      .format("jdbc")
      .option("url", databaseUrl)
      .option("dbtable", "sales")
      .load()
      .withColumnRenamed("product_name_hash", "sales_product_name_hash")

    val salesFilteredByDates = sales.filter(
      col("receipt_date") >= dateFrom
    ).filter(
      col("receipt_date") <= dateTo
    )

    val joinCondition = product.col("product_name_hash") === sales.col("sales_product_name_hash")
    val salesByBrand = product.join(
      salesFilteredByDates,
      joinCondition,
      "left_outer"
    )
      .groupBy("brand")
      .agg(
        round(
          sum("total_sum"),
          2
        ).as("total_sum"))
      .withColumn(
        "total_sum_pct",
        round(
          col("total_sum") / sum("total_sum").over(),
          2
        )
      )
    salesByBrand.show()
  }
}
