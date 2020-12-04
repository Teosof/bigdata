package LabThree

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
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

    df.show(false)

    val plot1 = Vegas("Job_Info").
      withDataFrame(df).
      encodeX("Dept", Nom).
      encodeY("Annual_Rt", Quant).
      mark(Bar)

    plot1.show

    val plot2 = Vegas("Job_Info").
      withDataFrame(df).
      encodeX("Unit", Nom).
      encodeY("Hrly_Rate", Quant).
      mark(Bar)

    plot2.show

    //    val df: Dataset[String] = spark.sql(s"SELECT * FROM csv.`$PATH/var.csv`").toJSON
    //    val df: Array[Row] = spark.sql(s"SELECT * FROM csv.`$PATH/var.csv`").collect()
    //    val df: DataFrame = spark.sql(s"SELECT * FROM csv.`$PATH/var.csv`")

    
  }
}
