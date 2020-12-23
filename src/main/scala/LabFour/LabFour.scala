package LabFour

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.StringIndexer
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

    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", true)
      .load(s"$PATH/var.csv")
    df.show(false)

    //val mapper = df.dropDuplicates("Unit").select("Unit", "FID").toDF("DistUnit", "UnitId")
    import spark.implicits._
    val seq = Seq((0, "CTMGR"),(1, "ATTOR"))
    val mapper = seq.toDF("UnitId", "DistUnit")

    val mappedDf = df.join(mapper,df("Unit") === mapper("DistUnit"),"inner" )
    val cols = Array("Annual_Rt", "Hrly_Rate")

    // VectorAssembler to add feature column
    // input columns - cols
    // feature column - features
    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")
    val featureDf = assembler.transform(mappedDf)

    val indexer = new StringIndexer()
      .setInputCol("UnitId")
      .setOutputCol("label")

    val labelDf = indexer.fit(featureDf).transform(featureDf)
    labelDf.show(false)

    val seed = 5043
    val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

    // train logistic regression model with training data set
    val logisticRegression = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.02)
      .setElasticNetParam(0.8)
    val logisticRegressionModel = logisticRegression.fit(trainingData)

    // run model with test data set to get predictions
    // this will add new columns rawPrediction, probability and prediction
    val predictionDf = logisticRegressionModel.transform(testData)
    predictionDf.show(10)

  }
}
