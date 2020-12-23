package LabFour

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
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

    val dataframe: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", value = true)
      .load(s"$PATH/var.csv")

    dataframe.show(false)

    val seq: Seq[(Int, String)] = Seq(
      (0, "WHITE NON HISPANIC"),
      (1, "ASIAN AND PACIFIC ISLANDER"),
      (2, "HISPANIC"),
      (3, "BLACK NON HISPANIC")
    )

    import spark.implicits._
    val mapper: DataFrame = seq.toDF("Ethnicity", "DistEthnicity")

    val mapped: DataFrame = dataframe
      .join(
        mapper, dataframe("Ethnicity") === mapper("DistEthnicity"),
        "inner"
      )
    val columns: Array[String] = Array("Count", "Rank")

    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(columns)
      .setOutputCol("features")
    val feature: DataFrame = assembler.transform(mapped)

    val indexer: StringIndexer = new StringIndexer()
      .setInputCol("DistEthnicity")
      .setOutputCol("label")

    val label: DataFrame = indexer
      .fit(feature)
      .transform(feature)
    label.show(false)

    val seed: Int = 5043
    val Array(training, test) = label
      .randomSplit(Array(0.7, 0.3), seed)

    val regression: LogisticRegression = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.02)
      .setElasticNetParam(0.8)
    val model: LogisticRegressionModel = regression
      .fit(training)

    val prediction: DataFrame = model
      .transform(test)
    prediction.show(10)
  }
}
