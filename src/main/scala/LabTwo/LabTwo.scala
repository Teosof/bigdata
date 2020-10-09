package LabTwo

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.tartarus.snowball.ext.russianStemmer

object LabTwo {
  val PATH: String = "src/main/data"
  val NODES: Int = 3

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Lab2").setMaster(s"local[$NODES]")
    val sc: SparkContext = new SparkContext(conf)
    LogManager.getRootLogger.setLevel(WARN)

    val book: RDD[String] = sc.textFile(s"$PATH/var.txt")
    val stop: Array[String] = sc.textFile(s"$PATH/stop.txt").collect()
    val text: RDD[(String, Int)] = parse(book = book, stop = stop)

    println("\n Top50 most common words: ")
    val most: Array[(String, Int)] = popular(text = text, ascending = false)
    most.foreach(println)

    println("\n Top50 least common words: ")
    val least: Array[(String, Int)] = popular(text = text, ascending = true)
    least.foreach(println)

    val stemmed: RDD[((String, Iterable[String]), Int)] = text
      .mapPartitions(stemming)
      .groupBy(_._2)
      .map(w => (w._1, w._2.map(_._1._1)) -> w._2.size)

    println("\n Top50 most common stems: ")
    val mostStemmed: Array[((String, Iterable[String]), Int)] = stemmed
      .sortBy(_._2, ascending = false)
      .take(50)
    mostStemmed.foreach(println)

    println("\n Top50 least common stems: ")
    val leastStemmed: Array[((String, Iterable[String]), Int)] = stemmed
      .sortBy(_._2, ascending = true)
      .take(50)
    leastStemmed.foreach(println)
  }

  private def stemming(iter: Iterator[(String, Int)]): Iterator[((String, Int), String)] = {
    val stemmer: russianStemmer = new russianStemmer
    iter.map(w => (w._1, 1) -> {
      stemmer.setCurrent(w._1)
      stemmer.stem
      stemmer.getCurrent
    })
  }

  private def popular(text: RDD[(String, Int)], ascending: Boolean): Array[(String, Int)] = {
    text.sortBy(_._2, ascending = ascending).take(num = 50)
  }

  private def parse(book: RDD[String], stop: Array[String]): RDD[(String, Int)] = book
    .flatMap(_.toLowerCase.split(" ")
      .map(_.replaceAll("[;:,.!?\"«» “–]", ""))
      .filter(word => word.length > 1 && !stop.contains(word)))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
}
