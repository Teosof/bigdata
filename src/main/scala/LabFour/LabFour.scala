package LabFour

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, LinearRegressionTrainingSummary}
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

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
    //df.select( df("Annual_Rt").cast(IntegerType).as("Annual_Rt") )
    val forVector = df.select("Unit","Annual_Rt", "Hrly_Rate").limit(4500)

    val assembler = new VectorAssembler()
      .setInputCols(Array("Annual_Rt", "Hrly_Rate"))
      .setOutputCol("features")
    val training = assembler.transform(forVector).select("Unit", "features").toDF("label", "features")
    training.show(false)

    // Create a LogisticRegression instance. This instance is an Estimator.
    val lr = new LogisticRegression()
    // Print out the parameters, documentation, and any default values.
    println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")

    // We may set parameters using setter methods.
    lr.setMaxIter(10)
      .setRegParam(0.01)

    // Learn a LogisticRegression model. This uses the parameters stored in lr.
    val model1 = lr.fit(training)
    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this
    // LogisticRegression instance.
    println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap}")

    // We may alternatively specify parameters using a ParamMap,
    // which supports several methods for specifying parameters.
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.

    // One can also combine ParamMaps.
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    val model2 = lr.fit(training, paramMapCombined)
    println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap}")

    // Prepare test data.
    val forTestVector = df.sort(col("FID").desc).select("Unit", "Hrly_Rate").limit(500)
    val test = assembler.transform(forTestVector).select("Unit","features").toDF("label", "features")
    test.show(false)
//      spark.createDataFrame(Seq(
//      (1.0, Vectors.dense(-1.0, 1.5)),
//      (0.0, Vectors.dense(3.0, 2.0)),
//      (1.0, Vectors.dense(0.0, 2.2))
//    )).toDF("label", "features")

    df.show(false)
    training.show(false)
    test.show(false)

    // Make predictions on test data using the Transformer.transform() method.
    // LogisticRegression.transform will only use the 'features' column.
    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
    model2.transform(test)
      .select("features", "label", "myProbability", "prediction")
      .collect()
      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }

  }
}
