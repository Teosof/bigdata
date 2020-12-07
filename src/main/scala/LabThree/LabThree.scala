package LabThree

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{countDistinct, _}
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

    dataframe.show(false)

    // Correlation between Ethnicity and Count
    Vegas("Children_Info")
      .withDataFrame(dataframe)
      .encodeX(field = "Ethnicity", dataType = Nominal)
      .encodeY(field = "Count", dataType = Quantitative, aggregate = AggOps.Max)
      .mark(Bar)
      .show

    // Correlation between Ethnicity and Rank
    Vegas("Children_Info")
      .withDataFrame(dataframe)
      .encodeX(field = "Ethnicity", dataType = Nominal)
      .encodeY(field = "Rank", dataType = Quantitative, aggregate = AggOps.Mean)
      .encodeColor(field = "Ethnicity", dataType = Nominal)
      .mark(Circle)
      .show

    // Correlation between Count and Rank
    Vegas("Children_Info")
      .withDataFrame(dataframe)
      .encodeX(field = "Count", dataType = Quantitative, sortOrder = SortOrder.Asc)
      .encodeY(field = "Rank", dataType = Quantitative)
      .mark(Line)
      .show

    // Number of unique ethnic groups
    dataframe.agg(countDistinct("Ethnicity")).show()
    // Top 20 most popular names
    dataframe.groupBy("Child's First Name").count().sort(col("count").desc).show()
  }
}
