package dataframes

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class Sales(spark: SparkSession, databaseUrl: String) extends BaseDataframe(spark: SparkSession) {
  override def getDF: DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", databaseUrl)
      .option("dbtable", "sales")
      .load()
      .withColumnRenamed("product_name_hash", "sales_product_name_hash")
  }

  def filterByDates(dateFrom: String, dateTo: String): Unit = {
    df.filter(
      col("receipt_date").between(dateFrom, dateTo)
    )
  }

  def filterByCategories(categories: String): DataFrame = {
    if (!"".equals(categories)) {
      val kktCategories = new KktCategories(spark, databaseUrl)
      val kktNumbers = kktCategories.getKktNumbers(categories)
      val joinCondition = df.col("kkt_number") === kktNumbers.col("number")
      df.join(
        kktNumbers,
        joinCondition,
        "inner"
      )
    } else df
  }
}
