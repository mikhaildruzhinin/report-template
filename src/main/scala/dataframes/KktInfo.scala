package dataframes

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KktInfo(val df: DataFrame) extends BaseDataFrame {
  def this(spark: SparkSession, databaseUrl: String) = this(KktInfo.getDF(spark, databaseUrl))

  def filterByActivity(kktActivity: KktActivity): KktInfo = {
    val joinCondition = this.df.col("number") === kktActivity.df.col("kkt_number")
    val df = this.df.join(
      kktActivity.df,
      joinCondition,
      "inner"
    )
    new KktInfo(df)
  }

  def calculateChannels: KktInfo = {
    val df = this.df.withColumn(
      "shop_count",
      sum("shop_id").over(
        Window.partitionBy("org_inn")
      )
    ).withColumn(
      "channel",
      when(
        col("shop_count") >= 3, "chain"
      ).otherwise("nonchain")
    ).drop("shop_count")
    new KktInfo(df)
  }
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
