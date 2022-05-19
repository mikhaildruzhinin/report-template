package report

import com.typesafe.config.Config
import util.CommandLineConf

class ReportBuilder(commandLineConf: CommandLineConf, applicationConf: Config) {
  val pathToProductsFile: String = commandLineConf.pathToProductsFile()
  val databaseUrl: String = applicationConf.getString("database.url")
  val dateFrom: String = commandLineConf.dateFrom()
  val dateTo: String = commandLineConf.dateTo()
  val categories: String = commandLineConf.categories()
}
