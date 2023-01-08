package com.github.mikhaildruzhinin.rt.util

import org.rogach.scallop.{ScallopConf, ScallopOption}

class CommandLineConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val pathToProductsFile: ScallopOption[String] = opt[String](
    name = "pathToProductsFile",
    short = 'p',
    required = true,
    descr = "Path to product_names.csv"
  )
  val dateFrom: ScallopOption[String] = opt[String](
    name = "dateFrom",
    short = 'f',
    required = true,
    descr = "Starting date for the report"
  )
  val dateTo: ScallopOption[String] = opt[String](
    name = "dateTo",
    short = 't',
    required = true,
    descr = "Final date for the report"
  )
  val categories: ScallopOption[String] = opt[String](
    name = "categories",
    short = 'c',
    default = Some(""),
    descr = "Filter by kkt_category"
  )
  val receiptDate: ScallopOption[Boolean] = opt[Boolean](
    name = "receiptDate",
    short='d',
    default = Some(false),
    descr = "Group by receipt_date"
  )
  val region: ScallopOption[Boolean] = opt[Boolean](
    name = "region",
    short = 'r',
    default = Some(false),
    descr = "Group by region"
  )
  val channel: ScallopOption[Boolean] = opt[Boolean](
    name = "channel",
    short = 'l', // 'c' and 'h' have been already taken
    default = Some(false),
    descr = "Group by channel"
  )
  verify()
}
