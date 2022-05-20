package report

import dataframes.{Products, Sales}
import org.apache.spark.sql.SparkSession

class Report(spark: SparkSession, builder: ReportBuilder) {
  private val pathToProductsFile: String = builder.pathToProductsFile
  private val databaseUrl: String = builder.databaseUrl
  private val dateFrom: String = builder.dateFrom
  private val dateTo: String = builder.dateTo
  private val categories: String = builder.categories
  private val groupByReceiptDate: Boolean = builder.groupByReceiptDate

  def generate(): Unit = {
    val products = new Products(spark, pathToProductsFile)

    val sales = new Sales(spark, databaseUrl)
    val salesFilteredByDates = new Sales(
      spark=spark,
      databaseUrl=databaseUrl,
      df=sales.filterByDates(dateFrom, dateTo)
    )
    val salesFilteredByCategories = new Sales(
      spark=spark,
      databaseUrl=databaseUrl,
      df=salesFilteredByDates.filterByCategories(categories)
    )

    val aggregateByColumns = Map(
      "receipt_date" -> groupByReceiptDate,
      "brand" -> true
    )
    val aggregatedResults = products.aggregateByBrand(
      salesFilteredByCategories,
      aggregateByColumns
    )
    aggregatedResults.show()
  }
}
