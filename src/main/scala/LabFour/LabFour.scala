package LabFour

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
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
      .withColumn("Child's First Name", lower(col("Child's First Name")))

    dataframe.show(false)

    // Indexing some columns for using as features
    val ethnicity: DataFrame = new StringIndexer()
      .setInputCol("Ethnicity")
      .setOutputCol("indexEthnicity")
      .fit(dataframe)
      .transform(dataframe)

    // Vectorization of required columns
    val cols: Array[String] = Array("indexEthnicity", "Count", "Rank")
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")
    val feature: DataFrame = assembler
      .transform(ethnicity)

    // Renaming columns for suiting SparkML
    val indexer: StringIndexer = new StringIndexer()
      .setInputCol("Ethnicity")
      .setOutputCol("label")
    val label: DataFrame = indexer
      .fit(feature)
      .transform(feature)
    label.show(false)

    // Splitting dataframe into training and test dataframes
    val seed: Int = 5043
    val Array(training, test) = label.randomSplit(Array(0.7, 0.3), seed)

    println(s"dataframe count: ${dataframe.count()}")
    println(s"training count: ${training.count()}")
    println(s"test count: ${test.count()}\n")

    // Train RandomForestClassifier model with training data set
    // Setting max feature bins at 330
    val regression: RandomForestClassifier = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxBins(330)
    val model: RandomForestClassificationModel = regression
      .fit(training)

    // Run model with test data set to get predictions
    // This will add new columns rawPrediction, probability and prediction
    val prediction: DataFrame = model
      .transform(test)
    prediction
      .select("Ethnicity", "Count", "Rank", "label", "prediction")
      .distinct()
      .show(2000)

    // Select (prediction, label) and compute accuracy.
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy: Double = evaluator
      .evaluate(prediction)

    // Round accuracy up to 2 digits
    println(
      s"Accuracy = ${
        BigDecimal(accuracy)
          .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      }"
    )
  }
}
