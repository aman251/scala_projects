package com.amanhardeep.spark


import net.liftweb.json._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
import org.apache.avro.Schema
import org.apache.spark.sql.types._
import scala.io.Source

import org.apache.avro.generic.{ GenericDatumReader, GenericRecord }
import org.apache.avro.file.DataFileReader
import scala.tools.nsc.transform.Flatten
import org.apache.spark.sql.functions.concat_ws

import scala.collection.mutable.ArrayBuffer

import com.amazonaws.services.s3._

object SavingAVROFormat {

case class myFileReader(filetype: String, filename: String, path: String, delimiter: String, quote: String, header: String, inferSchema: String)
case class myFields(name: String, mytype: String)
    
    def cast(datatype: String) : DataType = {
          datatype match {
            case "int" => IntegerType
            case "string" => StringType
          }
        }
  
    def main(args: Array[String]) {
      // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    val json: String = Source.fromFile("../friendsSchema.avrc").mkString

    println(json)
    
    implicit val formats = DefaultFormats
    
    val jsonaman = parse(json)
    val m = jsonaman.extract[myFileReader]    
    println(m.filetype)
    println(m.path)

    val fileDelimiter = m.delimiter.toString     
    val filePath = m.path.toString
    
    
    val readfields = (jsonaman \\ "fields").children
    val arrFields = ArrayBuffer[StructField]()
    
    for (i <- 0 until 4) {
      val e = readfields(0)(i)
      val values = e.values.asInstanceOf[Map[String, String]]
      println((values("name").toString + "," + (values("mytype")) + "," + true))
      arrFields.append(StructField(values("name").toString , cast(values("mytype")), true))         
    }
     arrFields.foreach(println)  
     val mynewSchema = StructType(arrFields.toList)
     println (mynewSchema)
        
//    val jsonData = JSON.parseRaw(json)
//    val jsonData = spark.read.json(spark.sparkContext.wholeTextFiles("../friendsSchema.avrc").values)
//    println("text from json")
//    val fileDelimiter = jsonData.select("delimiter").collect()(0)(0).toString      
//    val filePath = jsonData.select("path").collect()(0)(0).toString
//    val fileFields = jsonData.select("fields").collect()(0).getList(0).toArray()
//    val mySchema = StructType(StructField("myID", IntegerType, true)::StructField("myName", StringType, true)::StructField("myAge", IntegerType, true)::
//        StructField("myCount", IntegerType, true):: Nil)

   
     val str1 =  StructField("myID", IntegerType, true)::StructField("myName", StringType, true)::StructField("myAge", IntegerType, true)::
        StructField("myCount", IntegerType, true):: Nil
    
//    println(str1)
    
    val mySchema = StructType(str1)
     
    val df = spark.read.format("com.databricks.spark.csv").option("delimiter", fileDelimiter).schema(mynewSchema).load(filePath)
    
    
    println("Dataframe Schema")
    df.printSchema()
    
    df.show()
    
    df.write.format("com.databricks.spark.avro").save("../fakeavro.avro")
    
    println("Saved the file in AVRO format")
    
    println("trying to read it now")


    val df2 = spark.read.format("com.databricks.spark.avro").load("../fakeavro.avro")
    df2.printSchema()
    
    println(mySchema.json)
        
    }
}
