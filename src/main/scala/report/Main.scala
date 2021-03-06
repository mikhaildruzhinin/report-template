package report

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import util.{CommandLineConf, ReportBuilder}

object Main {
  def main(args: Array[String]): Unit = {
    val commandLineConf = new CommandLineConf(args)
    val applicationConf: Config = ConfigFactory.load("application.conf")

    val appName = applicationConf.getString("spark.appname")
    val sparkMaster = applicationConf.getString("spark.master")

    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.master", sparkMaster)
      .getOrCreate()

    val reportBuilder = new ReportBuilder(commandLineConf, applicationConf)
    val report = new Report(spark, reportBuilder)
    report.generate()
  }
}
