package LabThree

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions._
import vegas._
import vegas.sparkExt._


object LabThree {
  val PATH: String = "src/main/data"
  val NODES: Int = 3

  def main(args: Array[String]): Unit = {
    //Spark initialization
    val spark: SparkSession = SparkSession.builder()
      .appName("Lab3")
      .master(s"local[$NODES]")
      .getOrCreate
    LogManager.getRootLogger.setLevel(WARN)

    //Reading dataframe from file
    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(s"$PATH/var.csv")

    df.show(false)

    //Plot of maximum Annual rate for each department
    Vegas("Job_Info").
      withDataFrame(df).
      encodeX("Dept", Nom).
      encodeY("Annual_Rt", Quant, aggregate = AggOps.Max).
      mark(Bar).
      show

    //Boxplot of hourly rate for each unit
    Vegas("Job_Info").
      withDataFrame(df).
      encodeX("Unit", Nom).
      encodeY("Hrly_Rate", Quant).
      encodeColor("Unit", Nom).
      mark(Circle).
      show

    //Plot of average hourly rate for each job
    Vegas("Job_Info").
      withDataFrame(df).
      encodeX("Job_Title", Nom, sortOrder=SortOrder.Asc).
      encodeY("Hrly_Rate", Quant, aggregate = AggOps.Mean).
      mark(Line).
      show

    //Distinct number of units
    df.agg(countDistinct("Unit")).show()
    //Average annual rate
    df.select(avg("Annual_Rt")).show()
  }
}
