package br.com.marto.nlp.collocation.utils

import scala.io.Source

class Cleaner {
  val stopWords = Source.fromFile("./StopWords.txt").getLines.toList.distinct
  def clear(text:String): String = {
    var newText = text
    newText = newText.replaceAll("((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\\\\\))+[\\\\w\\\\d:#@%/;$()~_?\\\\+-=\\\\\\\\\\\\.&]*)", " ").toLowerCase()
    newText = newText.replaceAll("[\\p{Punct}]", " ").toLowerCase()
    newText = newText.replaceAll("\\s\\s+", " ").toLowerCase()
    newText = newText.replaceAll("\n", " ").toLowerCase()
    newText = newText.replaceAll("\"", " ").toLowerCase()
    stopWords.foreach(a => {newText = newText.replace(" " + a.trim + " ", " ")})
    newText
  }
}
