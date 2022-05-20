package dataframes

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class Sales(spark: SparkSession, databaseUrl: String, val df: DataFrame) extends BaseDataFrame {
  def this(spark: SparkSession, databaseUrl: String) = this(
    spark,
    databaseUrl,
    Sales.getDF(spark, databaseUrl)
  )

  def filterByDates(dateFrom: String, dateTo: String): DataFrame = {
    df.filter(
      col("receipt_date").between(dateFrom, dateTo)
    )
  }

  def filterByCategories(categories: String): DataFrame = {
    if (!"".equals(categories)) {
      val kktNumbers = new KktCategories(spark, databaseUrl).getKktNumbers(categories)
      val joinCondition = df.col("kkt_number") === kktNumbers.col("number")
      df.join(
        kktNumbers,
        joinCondition,
        "inner"
      )
    } else df
  }
}

object Sales {
  def getDF(spark: SparkSession, databaseUrl: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", databaseUrl)
      .option("dbtable", "sales")
      .load()
      .withColumnRenamed("product_name_hash", "sales_product_name_hash")
  }
}
