package com.github.mikhaildruzhinin.rt.util

import com.github.mikhaildruzhinin.rt.util.config.AppConfig

class ReportBuilder(commandLineConf: CommandLineConf, appConfig: AppConfig) {
  val pathToProductsFile: String = commandLineConf.pathToProductsFile()
  val databaseUrl: String = appConfig.database.url
  val dateFrom: String = commandLineConf.dateFrom()
  val dateTo: String = commandLineConf.dateTo()
  val categories: String = commandLineConf.categories()
  val groupByReceiptDate: Boolean = commandLineConf.receiptDate()
  val groupByRegion: Boolean = commandLineConf.region()
  val groupByChannel: Boolean = commandLineConf.channel()
  val datetimeFormat: String = appConfig.report.datetimeFormat
  val outputPath: String = appConfig.report.outputPath
}
