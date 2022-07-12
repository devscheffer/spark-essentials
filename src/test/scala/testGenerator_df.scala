import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import playground.generator_df.{SingleColumnData, df_generate}

case object TestGenerator_df extends App {
  /**
    * This creates a SparkSession, which will be used to operate on the DataFrames that we create.
    */
  val spark = SparkSession.builder()
    .appName("Spark App")
    .config("spark.master", "local")
    .getOrCreate()

  val base_dict = Seq(
    SingleColumnData("c1", IntegerType, Seq(0, 1)),
    SingleColumnData("c2", IntegerType, Seq(0, 1)),
  )
  val df4 = df_generate(base_dict, spark)
  df4.show()
}