package LabTwo

import org.apache.log4j.Level.WARN
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.tartarus.snowball.ext.russianStemmer

object LabTwo {
  val PATH: String = "src/main/data"
  val NODES: Int = 3

  // TODO: map partions
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Lab2").setMaster(s"local[$NODES]")
    val sc: SparkContext = new SparkContext(conf)
    LogManager.getRootLogger.setLevel(WARN)

    val book: RDD[String] = sc.textFile(s"$PATH/var.txt")
    //    val source: BufferedSource = Source.fromFile(s"$PATH/stop.txt")
    //    val stop: Array[String] = try source.mkString.split("\n") finally source.close()
    val stop: Array[String] = Array("бы", "он", "быть", "в", "весь", "вот", "все", "всей", "что", "он", "как", "но", "это", "не", "на", "его", "же", "так", "да", "вы", "она", "было", "еще")
    val text: RDD[(String, Int)] = parse(book = book, stop = stop)

    //    println("Top50 most common words: ")
    //    val most: Array[(String, Int)] = popular(text = text, ascending = false)
    //    most.foreach(println)

    //    println("Top50 least common words: ")
    //    val least: Array[(String, Int)] = popular(text = text, ascending = true)
    //    least.foreach(println)

    val stemmer = new russianStemmer
    stemmer.setCurrent("книга и книженция, книга и книжечка, книга и книгище")
    if (stemmer.stem) {
      val stemText: String = stemmer.getCurrent
      println(stemText)
    }
  }

  private def popular(text: RDD[(String, Int)], ascending: Boolean): Array[(String, Int)] = {
    text.sortBy(_._2, ascending = ascending).take(num = 50)
  }

  private def parse(book: RDD[String], stop: Array[String]): RDD[(String, Int)] = book
    .flatMap(_.toLowerCase.split(" ")
      .filter(word => word.length > 1 && !stop.contains(word)))
    .map(_.replaceAll("[,.!?\"«» “–]", ""))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
}
