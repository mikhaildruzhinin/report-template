package dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}

class KktInfo(val df: DataFrame) extends BaseDataFrame {
  def this(spark: SparkSession, databaseUrl: String) = this(KktInfo.getDF(spark, databaseUrl))
}

object KktInfo {
  def getDF(spark: SparkSession, databaseUrl: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", databaseUrl)
      .option("dbtable", "kkt_info")
      .load()
      .withColumnRenamed("kkt_number", "number")
  }
}
