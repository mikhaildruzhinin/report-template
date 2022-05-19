package dataframes

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KktCategories(spark: SparkSession, databaseUrl: String) extends BaseDataframe(spark: SparkSession) {
  override def getDF: DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", databaseUrl)
      .option("dbtable", "kkt_categories")
      .load()
  }

  def getKktNumbers(categories: String): DataFrame = {
    df
      .filter(col("category").isInCollection(categories.split(", ")))
      .withColumn(
        "rn",
        row_number().over(
          Window.partitionBy("kkt_number").orderBy(desc("version"))
        )
      )
      .filter(col("rn") === 1)
      .select("kkt_number")
      .withColumnRenamed("kkt_number", "number")
  }
}
