package report

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object Report {
  def main(args: Array[String]): Unit = {
    val commandLineConf = new CommandLineConf(args)
    val applicationConf: Config = ConfigFactory.load("application.conf")

    val appName = applicationConf.getString("spark.appname")
    val sparkMaster = applicationConf.getString("spark.master")

    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.master", sparkMaster)
      .getOrCreate()

    val reportGeneratorBuilder = new ReportGeneratorBuilder(commandLineConf, applicationConf)
    val reportGenerator = new ReportGenerator(spark, reportGeneratorBuilder)
    reportGenerator.generate()
  }
}
