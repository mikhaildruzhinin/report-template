package dataframes

import org.apache.spark.sql.DataFrame

abstract class BaseDataFrame {
  val df: DataFrame
}
