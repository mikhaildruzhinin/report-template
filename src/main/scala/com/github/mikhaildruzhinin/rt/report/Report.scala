package com.github.mikhaildruzhinin.rt.report

import com.github.mikhaildruzhinin.rt.dataframes.{KktActivity, KktCategories, KktInfo, Products, Sales}
import com.github.mikhaildruzhinin.rt.util.{ReportBuilder, ReportWriter}
import org.apache.spark.sql.SparkSession

class Report(spark: SparkSession, builder: ReportBuilder) {
  private val pathToProductsFile: String = builder.pathToProductsFile
  private val databaseUrl: String = builder.databaseUrl
  private val dateFrom: String = builder.dateFrom
  private val dateTo: String = builder.dateTo
  private val categories: String = builder.categories
  private val aggregateByColumns = Map(
    "receipt_date" -> builder.groupByReceiptDate,
    "region" -> builder.groupByRegion,
    "channel" -> builder.groupByChannel,
    "brand" -> true
  )
  private val datetimeFormat: String = builder.datetimeFormat
  private val outputPath: String = builder.outputPath

  def generate(): Unit = {
    val sales = new Sales(spark, databaseUrl)
    val products = new Products(spark, pathToProductsFile)
    val kktCategories = new KktCategories(spark, databaseUrl)
    val kktInfo = new KktInfo(spark, databaseUrl)
    val kktActivity = new KktActivity(spark, databaseUrl)

    val salesFilteredByDates = sales.filterByDates(dateFrom, dateTo)
    val salesFilteredByCategories = salesFilteredByDates.filterByCategories(categories, kktCategories)

    val kktActivityFilteredByDates = kktActivity.filterByDates(dateFrom, dateTo)
    val activeKktInfo = kktInfo.filterByActivity(kktActivityFilteredByDates)
    val activeKktInfoWithChannels = activeKktInfo.calculateChannels

    val salesWithBrands = salesFilteredByCategories.joinSalesAndBrands(products)
    val salesWithInfo = salesWithBrands.joinSalesAndInfo(aggregateByColumns, activeKktInfoWithChannels)

    val columnsToGroupBy = aggregateByColumns.filter(_._2).keySet.toArray

    val totalSum = salesWithInfo.calculateTotalSum(columnsToGroupBy)
    val totalSumPct = totalSum.calculateTotalSumPct(columnsToGroupBy)

    val reportWriter = new ReportWriter(datetimeFormat, outputPath)
    reportWriter.write(totalSumPct)
  }
}
