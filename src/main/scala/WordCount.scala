package com.amanhardeep.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object WordCount {
    
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCount")
    
    val lines = sc.textFile("../book.txt")
    val words = lines.flatMap(x => x.split("\\W+")).map(x=>x.toLowerCase)
        
    val wordsrdd = words.map(x => (x,1))
    
    val wordsCount = wordsrdd.reduceByKey((x,y) => (x+y))
    
    val sortedCount = wordsCount.map(x => (x._2, x._1)).sortByKey(ascending = false) 
    val result = sortedCount.collect()
    for (res <- result) {
      val word1 = res._2
      val count1 = res._1
      
      println(s"$word1  $count1")
    }
       
  }
  
  
}