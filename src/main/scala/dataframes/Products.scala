package dataframes

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, round, sum}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Products(spark: SparkSession, pathToProductsFile: String, val df: DataFrame) extends BaseDataFrame {
  def this(spark: SparkSession, pathToProductsFile: String) = this(
    spark,
    pathToProductsFile,
    Products.getDF(spark, pathToProductsFile)
  )

  def aggregateByBrand(sales: Sales, aggregateByColumns: Map[String, Boolean]): DataFrame = {
    val joinCondition = df.col("product_name_hash") === sales.df.col("sales_product_name_hash")

    val columnsToGroupBy = aggregateByColumns.filter(_._2).keySet.toArray

    df.join(
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

object Products {
  def getDF(spark: SparkSession, pathToProductsFile: String): DataFrame  = {
    val productSchema = StructType(
      Array(
        StructField("brand", StringType),
        StructField("product_name_hash", StringType)
      )
    )

    spark.read
      .schema(productSchema)
      .option("header", "true")
      .option("sep", ",")
      .csv(pathToProductsFile)
  }
}
