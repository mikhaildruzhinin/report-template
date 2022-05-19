package report

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object Report {
  def main(args: Array[String]): Unit = {
    val commandLineConf = new CommandLineConf(args)

    val pathToProductsFile = commandLineConf.pathToProductsFile()
    val dateFrom = commandLineConf.dateFrom()
    val dateTo = commandLineConf.dateTo()

    val applicationConf: Config = ConfigFactory.load("application.conf")

    val appName = applicationConf.getString("spark.appname")
    val sparkMaster = applicationConf.getString("spark.master")

    val databaseUrl = applicationConf.getString("database.url")

    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.master", sparkMaster)
      .getOrCreate()

    val reportGenerator = new ReportGenerator(spark, pathToProductsFile, databaseUrl, dateFrom, dateTo)
    reportGenerator.generate()
  }
}
