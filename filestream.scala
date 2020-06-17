package org.inceptez.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession

object filestream 
{
  
  def main(args:Array[String])
  {
    // Create the context
    // val sparkConf = new SparkConf().setAppName("textstream").setMaster("local[*]")
    //val sparkcontext = new SparkContext(sparkConf)
    
    
    val spark = SparkSession.builder.appName("textstream").enableHiveSupport.master("local[*]").getOrCreate();
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
   
    val ssc = new StreamingContext(sc,Seconds(10))
    val lines = ssc.textFileStream("file:///home/hduser/sparkdata/streaming/")
    val courses = lines.flatMap(_.split(" "))
    val coursesCounts = courses.map(x => (x, 1)).reduceByKey(_ + _)
    coursesCounts.print()
    ssc.start()
    ssc.awaitTermination()


  }
  
}