package util

import dataframes.BaseDataFrame

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class ReportWriter(datetimeFormat: String, outputPath: String) {
  def write[T <: BaseDataFrame](report: T): Unit = {
    val datetime = LocalDateTime.now()
    val formattedDatetime = DateTimeFormatter.ofPattern(datetimeFormat)
      .format(datetime)
    report.df.write
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .save(s"$outputPath/$formattedDatetime/report")
  }
}
