package com.amanhardeep.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min


object MinTemperature {

  def parseLines(lines:String) = {
    val fields = lines.split(",")
    val station = fields(0)
    val tempType = fields(2)
    val temperature = fields(3).toFloat * 0.1f *(9.0f / 5.0f) * 32.0f
    (station, tempType, temperature)
  }
 
  def displayResults(results: (String, Float)) = {
    val station = results._1
    val temp = results._2
    
    println(s"$station minimum temperature is $temp")
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "MinTemperature")
    val rdd = sc.textFile("../1800.csv")
    
    val parsedrdd = rdd.map(parseLines)
    val minTemps = parsedrdd.filter(x => x._1 == "TMIN")
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
    val minTempByStation = stationTemps.reduceByKey( (a, b) => min(a, b) )
    
    val results = minTempByStation.collect()
    

    
    
  }
  
  
  
  
}