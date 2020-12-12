package LabFour

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, LinearRegressionTrainingSummary}
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LabFour {
  val PATH: String = "src/main/data"
  val NODES: Int = 3


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Lab4")
      .master(s"local[$NODES]")
      .getOrCreate
    LogManager.getRootLogger.setLevel(WARN)

    val dataframe: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(s"$PATH/var.csv")
      .withColumn("Child's First Name", lower(col("Child's First Name")))

    val lr: LinearRegression = new LinearRegression()
      .setFeaturesCol("Count")
      .setLabelCol("Child's First Name")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val lrModel: LinearRegressionModel = lr.fit(dataframe)

    println(lrModel.coefficients)
    println(lrModel.intercept)
    val trainingSummary: LinearRegressionTrainingSummary = lrModel.summary
    println(trainingSummary.totalIterations)
    println(trainingSummary.objectiveHistory.mkString(","))
    trainingSummary.residuals.show()
    println(trainingSummary.rootMeanSquaredError)
    println(trainingSummary.r2)

  }
}
