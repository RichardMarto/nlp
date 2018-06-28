package br.com.marto.nlp.collocation

import br.com.marto.nlp.collocation.utils.Cleaner
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class Corpus(text: String) {

  val conf = new SparkConf().setAppName("collocation").setMaster("local[1]")
  val spark = new SparkContext(conf)
  val cleaner = new Cleaner()
  var newText = cleaner.clear(text)
  val splited = newText.split(" ")

  def process(): RDD[(String, Int)] = {
    val words = spark.parallelize(splited)
    var collocations: mutable.MutableList[String] = new mutable.MutableList
    var old: mutable.MutableList[String] = new mutable.MutableList
    for (i <- 0 until splited.length) {
      if (splited.length > i + 2) {
        val trigram = splited(i).trim + " " + splited(i+1).trim + " " + splited(i+2).trim
        if (!old.contains(trigram)){
          val r = trigram.r
          var matches = r.findAllIn(newText).foreach(collocations += _.toString)
        }
        old += trigram
      }
      if (splited.length > i + 1) {
        val bigram = splited(i).trim + " " + splited(i+1).trim
        if (!old.contains(bigram)){
          val r = bigram.r
          var matches = r.findAllIn(newText).foreach(collocations += _.toString)
        }
        old += bigram
      }
      val monogram = splited(i).trim
      if (monogram.length > 2) {
        if (!old.contains(monogram)){
          val r = monogram.r
          var matches = r.findAllIn(newText).foreach(collocations += _.toString)
        }
        old += monogram
      }

    }
    spark.parallelize(collocations.map(a => (a, a.split(" ").length))).reduceByKey(_+_).sortBy(_._2)

  }
  def getCollocations(): RDD[(String, Int)] = {
    process()
  }
}
