package LabTwo

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LabTwo {
  val PATH: String = "src/main/scala/LabTwo/var.txt"
  val NODES: Int = 3

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Lab2").setMaster(s"local[$NODES]")
    val sc: SparkContext = new SparkContext(conf)
    LogManager.getRootLogger.setLevel(Level.WARN)

    val rdd: RDD[String] = sc.textFile(PATH)
    rdd.collect().foreach(println)
  }
}
