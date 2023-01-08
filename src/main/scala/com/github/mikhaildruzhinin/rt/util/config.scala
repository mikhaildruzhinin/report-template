package com.github.mikhaildruzhinin.rt.util

object config {
  case class SparkConfig(appName: String,
                         master: String)

  case class DatabaseConfig(url: String)

  case class ReportConfig(datetimeFormat: String,
                          outputPath: String)

  case class AppConfig(spark: SparkConfig,
                       database: DatabaseConfig,
                       report: ReportConfig)
}
