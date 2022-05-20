package report

import dataframes.{KktCategories, KktInfo, Products, Sales}
import org.apache.spark.sql.SparkSession

class Report(spark: SparkSession, builder: ReportBuilder) {
  private val pathToProductsFile: String = builder.pathToProductsFile
  private val databaseUrl: String = builder.databaseUrl
  private val dateFrom: String = builder.dateFrom
  private val dateTo: String = builder.dateTo
  private val categories: String = builder.categories
  private val groupByReceiptDate: Boolean = builder.groupByReceiptDate
  private val groupByRegion: Boolean = builder.groupByRegion

  private def getColumnsToGroupBy: Array[String] = {
    val aggregateByColumns = Map(
      "receipt_date" -> groupByReceiptDate,
      "region" -> groupByRegion,
      "brand" -> true
    )
    aggregateByColumns.filter(_._2).keySet.toArray
  }

  def generate(): Unit = {
    val sales = new Sales(spark, databaseUrl)
    val products = new Products(spark, pathToProductsFile)
    val kktCategories = new KktCategories(spark, databaseUrl)
    val kktInfo = new KktInfo(spark, databaseUrl)

    val salesFilteredByDates = sales.filterByDates(dateFrom, dateTo)
    val salesFilteredByCategories = salesFilteredByDates.filterByCategories(categories, kktCategories)

    val salesByBrands = salesFilteredByCategories.joinSalesAndBrands(products)
    val salesByRegion = salesByBrands.joinSalesAndRegions(groupByRegion, kktInfo)

    val columnsToGroupBy = getColumnsToGroupBy

    val totalSum = salesByRegion.calculateTotalSum(columnsToGroupBy)
    val totalSumPct = totalSum.calculateTotalSumPct(columnsToGroupBy)
    totalSumPct.df.show()
  }
}
