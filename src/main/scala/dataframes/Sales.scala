package dataframes

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, round, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Sales(val df: DataFrame) extends BaseDataFrame {
  def this(spark: SparkSession, databaseUrl: String) = this(Sales.getDF(spark, databaseUrl))

  def filterByDates(dateFrom: String, dateTo: String): Sales = {
    val df = this.df.filter(
      col("receipt_date").between(dateFrom, dateTo)
    )
    new Sales(df)
  }

  def filterByCategories(categories: String, kktCategories: KktCategories): Sales = {
    val df = if (!"".equals(categories)) {
      val kktNumbers = kktCategories.getKktNumbersDF(categories)
      val joinCondition = this.df.col("kkt_number") === kktNumbers.col("number")
      this.df.join(
        kktNumbers,
        joinCondition,
        "inner"
      )
    } else this.df
    new Sales(df)
  }

  def joinSalesAndBrands(products: Products): Sales = {
    val joinCondition = this.df.col("product_name_hash") === products.df.col("name_hash")
    val df = this.df.join(
      products.df,
      joinCondition,
      "inner"
    )
    new Sales(df)
  }

  def joinSalesAndInfo(aggregateByColumns: Map[String, Boolean], kktInfo: KktInfo): Sales = {
    val df = if (
      aggregateByColumns.getOrElse("region", false)
        || aggregateByColumns.getOrElse("channel", false)
    ) {
      val joinCondition = this.df.col("kkt_number") === kktInfo.df.col("number")
      this.df.join(
        kktInfo.df,
        joinCondition,
        "inner"
      )
    } else {
      this.df
    }
    new Sales(df)
  }

  def calculateTotalSum(columnsToGroupBy: Array[String]): Sales = {
    val df = this.df.groupBy(columnsToGroupBy.head, columnsToGroupBy.tail: _*)
      .agg(
        round(
          sum("total_sum"),
          2
        ).as("total_sum"))
      .orderBy(columnsToGroupBy.head, columnsToGroupBy.tail: _*)
    new Sales(df)
  }

  def calculateTotalSumPct(columnsToGroupBy: Array[String]): Sales = {
    val df = this.df.withColumn(
      "total_sum_pct",
      round(
        col("total_sum") / sum("total_sum").over(
          if (columnsToGroupBy.length > 1) {
            Window.partitionBy(
              columnsToGroupBy.dropRight(1).head,
              columnsToGroupBy.dropRight(1).tail: _*
            )
          } else {
            Window.partitionBy() // can't get the head of an empty array
          }
        ),
        2
      )
    )
    new Sales(df)
  }
}

object Sales {
  def getDF(spark: SparkSession, databaseUrl: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", databaseUrl)
      .option("dbtable", "sales")
      .load()
  }
}
