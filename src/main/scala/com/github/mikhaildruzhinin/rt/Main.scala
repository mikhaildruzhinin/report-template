package com.github.mikhaildruzhinin.rt

import com.github.mikhaildruzhinin.rt.report.Report
import com.github.mikhaildruzhinin.rt.util.config.AppConfig
import com.github.mikhaildruzhinin.rt.util.{CommandLineConf, ReportBuilder}
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource.default.loadOrThrow
import pureconfig.generic.auto._

object Main {
  def main(args: Array[String]): Unit = {
    lazy val appConfig: AppConfig = loadOrThrow[AppConfig]

    val commandLineConf = new CommandLineConf(args)

    val spark = SparkSession.builder()
      .appName(appConfig.spark.appName)
      .config("spark.master", appConfig.spark.master)
      .getOrCreate()

    val reportBuilder = new ReportBuilder(commandLineConf, appConfig)
    val report = new Report(spark, reportBuilder)
    report.generate()
  }
}
