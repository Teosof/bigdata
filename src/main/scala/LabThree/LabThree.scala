package LabThree

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import vegas._
import vegas.sparkExt._


object LabThree {
  val PATH: String = "src/main/data"
  val NODES: Int = 3

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Lab3")
      .master(s"local[$NODES]")
      .getOrCreate
    LogManager.getRootLogger.setLevel(WARN)

    val dataframe: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(s"$PATH/var.csv")

    //    val dataframe: Dataset[String] = spark.sql(s"SELECT * FROM csv.`$PATH/var.csv`").toJSON
    //    val dataframe: Array[Row] = spark.sql(s"SELECT * FROM csv.`$PATH/var.csv`").collect()
    //    val dataframe: DataFrame = spark.sql(s"SELECT * FROM csv.`$PATH/var.csv`")

    //    dataframe.printSchema()
    //    dataframe.select("Gender").show()
    //    dataframe.groupBy("Gender").count().show()

    //    dataframe.select("*").show()

    //    dataframe.groupBy("Child's First Name").count().show()

    //    val plot = Vegas("Some plot", width = 400.0, height = 300.0)
    //      .withDataFrame(dataframe)
    //      .encodeX("field_x", Nom)
    //      .encodeY("field_y", Quant, aggregate = AggOps.Sum)
    //      .encodeColor(field = "field_t", dataType = Nominal,
    //        legend = Legend(orient = "left", title = "Type"))
    //      .encodeDetailFields(Field(field = "field_t", dataType = Nominal))
    //      .mark(Bar)
    //      .configMark(stacked = StackOffset.Zero)
    //        plot.show
    val plot: Unit = Vegas("approval date")
      .withDataFrame(dataframe)
      .mark(Bar)
      .encodeX("Hi Roman", Quant, bin = Bin(maxbins = 20.0), sortOrder = SortOrder.Desc)
      .show
  }
}
