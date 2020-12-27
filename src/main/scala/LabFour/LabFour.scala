package LabFour

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.classification.{RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer}

object LabFour {
  val PATH: String = "src/main/data"
  val NODES: Int = 3


  def main(args: Array[String]): Unit = {

    //SparkSession initialization
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Lab4")
      .master(s"local[$NODES]")
      .getOrCreate
    LogManager.getRootLogger.setLevel(WARN)

    //Reading .csv file into dataframe
    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", true)
      .load(s"$PATH/var.csv")
    df.show(false)
    println("dataframe count " + df.count())

    // Indexing some columns for using as features
    val unitIndexer = new StringIndexer()
      .setInputCol("Unit")
      .setOutputCol("indexUnit")
    val unitIndexedDf = unitIndexer.fit(df).transform(df)
    val deptIndexer = new StringIndexer()
      .setInputCol("Dept")
      .setOutputCol("indexDept")
    val deptIndexedDF = deptIndexer.fit(unitIndexedDf).transform(unitIndexedDf)
    val jobIndexer = new StringIndexer()
      .setInputCol("Job_Title")
      .setOutputCol("indexJob_Title")
    val jobIndexedDF = jobIndexer.fit(deptIndexedDF).transform(deptIndexedDF)

    //Vectorization of required columns
    val cols = Array("indexDept", "Annual_Rt", "Hrly_Rate")
    val assembler = new VectorAssembler ()
      .setInputCols(cols)
      .setOutputCol("features")
    val featureDf = assembler.transform(jobIndexedDF)

    //Renaming columns for suiting SparkML
    val indexer = new StringIndexer()
      .setInputCol("Unit")
      .setOutputCol("label")
    val labelDf = indexer.fit(featureDf).transform(featureDf)
    labelDf.show(false)

    //Splitting dataframe into training and test dataframes
    val seed = 5043
    val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

    println("trainingData count " + trainingData.count())
    println("testData count " + testData.count())

    // Train RandomForestClassifier model with training data set
    // Setting max feature bins at 330
    val Regression = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxBins(330)
    val model = Regression.fit(trainingData)

    // run model with test data set to get predictions
    // this will add new columns rawPrediction, probability and prediction
    val predictionDf = model.transform(testData)
    predictionDf.select("Dept", "Annual_Rt", "Hrly_Rate", "label", "prediction").show(50)

    // Select (prediction, label) and compute accuracy.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictionDf)
    //round accuracy up to 2 digits
    println(s"Accuracy = ${BigDecimal(accuracy).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble}")

  }
}
