package report

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, round, row_number, sum}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ReportGenerator(spark: SparkSession, builder: ReportGeneratorBuilder) {
  private val pathToProductsFile: String = builder.pathToProductsFile
  private val databaseUrl: String = builder.databaseUrl
  private val dateFrom: String = builder.dateFrom
  private val dateTo: String = builder.dateTo
  private val categories: String = builder.categories

 private def getProducts: DataFrame = {
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

  private def getSales: DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", databaseUrl)
      .option("dbtable", "sales")
      .load()
      .withColumnRenamed("product_name_hash", "sales_product_name_hash")
  }

  private def filterSalesByDates(sales: DataFrame): DataFrame = {
    sales.filter(
      col("receipt_date").between(dateFrom, dateTo)
    )
  }

  private def getKktCategories: DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", databaseUrl)
      .option("dbtable", "kkt_categories")
      .load()
  }

  private def getKktNumbersByCategory(kktCategories: DataFrame): DataFrame = {
    kktCategories
      .filter(col("category").isInCollection(categories.split(", ")))
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

  private def filterSalesByCategories(sales: DataFrame): DataFrame = {
    if (!"".equals(categories)) {
      val kktCategories = getKktCategories
      val kktNumbers = getKktNumbersByCategory(kktCategories)
      val joinCondition = sales.col("kkt_number") === kktNumbers.col("number")
      sales.join(
        kktNumbers,
        joinCondition,
        "inner"
      )
    } else sales
  }

  private def joinProductsAndSales(products: DataFrame, sales: DataFrame): DataFrame = {
    val joinCondition = products.col("product_name_hash") === sales.col("sales_product_name_hash")
    products.join(
      sales,
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

  def generate(): Unit = {
    val products = getProducts
    val sales = getSales
    val salesFilteredByDates = filterSalesByDates(sales)
    val salesFilteredByCategories = filterSalesByCategories(salesFilteredByDates)
    val salesByBrand = joinProductsAndSales(products, salesFilteredByCategories)
    salesByBrand.show()
  }
}
