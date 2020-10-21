package LabThree

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import vegas.DSL.ExtendedUnitSpecBuilder
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

    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(s"$PATH/var.csv")

    //    val df: Dataset[String] = spark.sql(s"SELECT * FROM csv.`$PATH/var.csv`").toJSON
    //    val df: Array[Row] = spark.sql(s"SELECT * FROM csv.`$PATH/var.csv`").collect()
    //    val df: DataFrame = spark.sql(s"SELECT * FROM csv.`$PATH/var.csv`")

    //    df.printSchema()
    //    df.select("Gender").show()
    //    df.groupBy("Gender").count().show()

    //    df.groupBy("Child's First Name").count().show()

    val plot: ExtendedUnitSpecBuilder = Vegas("Some histogram", width = 400.0, height = 300.0)
      .withDataFrame(df)
      .encodeX("field_x", Nom)
      .encodeY("field_y", Quant)
      .mark(Bar)
    plot.show
  }
}
