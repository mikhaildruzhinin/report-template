package dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, round, sum}

class AggregatedResults(spark: SparkSession, databaseUrl: String, products: Products, sales: Sales, aggregateByColumns: Map[String, Boolean]) extends BaseDataFrame {
  val df: DataFrame = {
    val columnsToGroupBy = aggregateByColumns.filter(_._2).keySet.toArray
    val groupByRegion = aggregateByColumns.getOrElse("region", false)

    val salesMatchedWithBrands = matchSalesAndBrands(products.df, sales.df)
    val salesMatchedByRegion = matchSalesAndRegions(salesMatchedWithBrands, groupByRegion)

    salesMatchedByRegion.groupBy(columnsToGroupBy.head, columnsToGroupBy.tail: _*)
      .agg(
        round(
          sum("total_sum"),
          2
        ).as("total_sum"))
      .orderBy(columnsToGroupBy.head, columnsToGroupBy.tail: _*)
      .withColumn(
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
  }

  private def matchSalesAndBrands(products: DataFrame, sales: DataFrame): DataFrame = {
    val joinCondition = sales.col("sales_product_name_hash") === products.col("product_name_hash")
    sales.join(
      products,
      joinCondition,
      "inner"
    )
  }

  private def matchSalesAndRegions(sales: DataFrame, groupByRegion: Boolean) = {
    if (groupByRegion) {
      val regions = new KktInfo(spark, databaseUrl).df
      val joinCondition = sales.col("kkt_number") === regions.col("number")
      sales.join(
        regions,
        joinCondition,
        "inner"
      )
    } else {
      sales
    }
  }
}
