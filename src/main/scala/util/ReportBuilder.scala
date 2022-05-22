package util

import com.typesafe.config.Config

class ReportBuilder(commandLineConf: CommandLineConf, applicationConf: Config) {
  val pathToProductsFile: String = commandLineConf.pathToProductsFile()
  val databaseUrl: String = applicationConf.getString("database.url")
  val dateFrom: String = commandLineConf.dateFrom()
  val dateTo: String = commandLineConf.dateTo()
  val categories: String = commandLineConf.categories()
  val groupByReceiptDate: Boolean = commandLineConf.receiptDate()
  val groupByRegion: Boolean = commandLineConf.region()
  val groupByChannel: Boolean = commandLineConf.channel()
  val datetimeFormat: String = applicationConf.getString("report.datetime_format")
  val outputPath: String = applicationConf.getString("report.output_path")
}
