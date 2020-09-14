package LabTwo

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LabTwo {
  val PATH: String = "src/main/scala/LabTwo/var.txt"
  val NODES: Int = 3

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Lab2").setMaster(s"local[$NODES]")
    val sc: SparkContext = new SparkContext(conf)
    LogManager.getRootLogger.setLevel(WARN)

    val rdd: RDD[String] = sc.textFile(PATH)


    val most: RDD[(Int, String)] = common_words(sc, rdd, order = true)
    val least: RDD[(Int, String)] = common_words(sc, rdd, order = false)
  }

  private def common_words(sc: SparkContext, rdd: RDD[String], order: Boolean): RDD[(Int, String)] = {
    // word count
    val wc: RDD[(String, Int)] = rdd.flatMap(l => l.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    // swap k,v to v,k to sort by word frequency
    val wc_swap: RDD[(Int, String)] = wc.map(_.swap)
    // sort keys by ascending=false (descending)
    val words: RDD[(Int, String)] = wc_swap.sortByKey(ascending = order, 1)
    // get an array of top 50 frequent words
    val top50: Array[(Int, String)] = words.take(num = 50)
    // convert array to RDD
    sc.parallelize(top50)
  }
}
