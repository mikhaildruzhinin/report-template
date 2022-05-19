package report

import dataframes.{Products, Sales}
import org.apache.spark.sql.SparkSession

class Report(spark: SparkSession, builder: ReportBuilder) {
  private val pathToProductsFile: String = builder.pathToProductsFile
  private val databaseUrl: String = builder.databaseUrl
  private val dateFrom: String = builder.dateFrom
  private val dateTo: String = builder.dateTo
  private val categories: String = builder.categories

  def generate(): Unit = {
    val products = new Products(spark, pathToProductsFile)

    val sales = new Sales(spark, databaseUrl)
    sales.filterByDates(dateFrom, dateTo)
    sales.filterByCategories(categories)

    val aggregatedResults = products.aggregateByBrand(sales)
    aggregatedResults.show()
  }
}
