package com.amanhardeep.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io.File
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials

object AwsS3Data {
  
  def connectAWS(bname: String): AmazonS3Client = {

    val AWS_ACCESS_KEY = "AKIAJI5W3PPVONISVT2A"   
    val AWS_SECRET_KEY = "ldbmtSgLsgTXGomPWJsnNJOd138iyHGXm8DwgY5i"
    
    println("set credentials")
    
    val yourAWSCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    
    println("creating S3 client")
    val amazonS3Client = new AmazonS3Client(yourAWSCredentials)
    
    println("created S3 object")
    return amazonS3Client
  }

  
  def main(args: Array[String]) {
      // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
    val spark = SparkSession.builder().master("local").getOrCreate()
    val filepath = "s3n:\\amanpreet251\fakefriends.csv"
    val delimiter = ","
    val mySchema = StructType(StructField("myID", IntegerType, true)::StructField("myName", StringType, true)::StructField("myAge", IntegerType, true)::
          StructField("myCount", IntegerType, true):: Nil)

    val bucketName = "amanpreet251"
    val amazonS3Client: AmazonS3Client = connectAWS(bucketName)
    println("AWS S3 object created")
          
    val df = spark.read.format("com.databricks.spark.csv").option("delimiter", delimiter).schema(mySchema).load(filepath)
    df.show()
    
     val fileToUpload = new File(filepath)
  
   
//  amazonS3Client.putObject(bucketName, "FakeFriends", fileToUpload)

  
  
  println("Transfer Completed")
  
     
    }
  
  
}