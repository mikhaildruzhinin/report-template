package dataframes

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KktActivity(val df: DataFrame) extends BaseDataFrame {
  def this(spark: SparkSession, databaseUrl: String) = this(KktActivity.getDF(spark, databaseUrl))

  def filterByDates(dateFrom: String, dateTo: String): KktActivity = {
    val df = this.df.withColumn("dateFrom", lit(dateFrom))
      .withColumn("dateTo", lit(dateTo))
      .filter(
      !(
        col("dateFrom") > col("receipt_date_max")
          || (col("receipt_date_min") > col("dateTo"))
        )
      ).drop("dateFrom", "dateTo")
    new KktActivity(df)
  }
}

object KktActivity {
  def getDF(spark: SparkSession, databaseUrl: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", databaseUrl)
      .option("dbtable", "kkt_activity")
      .load()
  }
}
