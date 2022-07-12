package playground

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object generator_df extends App {
  def df_generate(dataset: Seq[SingleColumnData], spark: SparkSession) = {
    val df_create = dataset.map(x => _list_to_df(x, spark))
    val df_reduce = df_create.reduce((x, y) => _cross_join_df(x, y))
    df_reduce
  }

  def _list_to_df(dataset: SingleColumnData, spark: SparkSession): DataFrame = {
    val dataSchema = StructType(Array(
      StructField(dataset.columnName, dataset.columnType),
    ))
    val dataRows = spark.sparkContext.parallelize(dataset.columnDataTreated)
    val df: DataFrame = spark.createDataFrame(dataRows, dataSchema)
    df
  }

  def _cross_join_df(df1: DataFrame, df2: DataFrame): DataFrame = {
    val df: DataFrame = df1.join(df2)
    df
  }

  case class SingleColumnData(columnName: String, columnType: DataType, columnData: Seq[Any]) {
    val columnDataTreated: Seq[Row] = columnData.map(x => Row(x))
  }
}
