package dataframes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, round, sum}

class AggregatedResults(products: Products, sales: Sales, aggregateByColumns: Map[String, Boolean]) extends BaseDataFrame {
  val df: DataFrame = {
    val joinCondition = products.df.col("product_name_hash") === sales.df.col("sales_product_name_hash")

    val columnsToGroupBy = aggregateByColumns.filter(_._2).keySet.toArray

    products.df.join(
      sales.df,
      joinCondition,
      "inner"
    ).groupBy(columnsToGroupBy.head, columnsToGroupBy.tail: _*)
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
}
