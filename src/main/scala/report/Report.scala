package report

import dataframes.{AggregatedResults, Products, Sales}
import org.apache.spark.sql.SparkSession

class Report(spark: SparkSession, builder: ReportBuilder) {
  private val pathToProductsFile: String = builder.pathToProductsFile
  private val databaseUrl: String = builder.databaseUrl
  private val dateFrom: String = builder.dateFrom
  private val dateTo: String = builder.dateTo
  private val categories: String = builder.categories
  private val groupByReceiptDate: Boolean = builder.groupByReceiptDate
  private val groupByRegion: Boolean = builder.groupByRegion

  def generate(): Unit = {
    val products = new Products(spark, pathToProductsFile)

    val sales = new Sales(spark, databaseUrl)
    val salesFilteredByDates = sales.filterByDates(dateFrom, dateTo)
    val salesFilteredByCategories = salesFilteredByDates.filterByCategories(categories)

    val aggregateByColumns = Map(
      "receipt_date" -> groupByReceiptDate,
      "region" -> groupByRegion,
      "brand" -> true
    )
    val aggregatedResults = new AggregatedResults(
      spark,
      databaseUrl,
      products,
      salesFilteredByCategories,
      aggregateByColumns
    )
    aggregatedResults.df.show()
  }
}
