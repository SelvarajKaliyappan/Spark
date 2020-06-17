package org.inceptez.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import java.lang.Math
import org.apache.spark.sql.SparkSession


object socketstream 
{
  def main(args:Array[String])
  {

    val sparkSession = SparkSession.builder.appName("auctionsocketstream").enableHiveSupport.master("local[*]").getOrCreate();
    val sparkcontext = sparkSession.sparkContext;
    sparkcontext.setLogLevel("ERROR")
    //index names
    val auctionid = 0
    val bid = 1
    val bidtime = 2
    val bidder = 3
    val bidderrate = 4
    val openbid = 5
    val price = 6
    val itemtype = 7
    val daystolive = 8
    // Create the context
    val ssc = new StreamingContext(sparkcontext, Seconds(20))
    val lines = ssc.socketTextStream("localhost", 9999)
    val auctionRDD = lines.map(line => line.split("~"))
    val items_auctionRDD = auctionRDD.map(x => (x(itemtype), 1)).reduceByKey((x, y) => x + y)
    //Identify which item has more auction response
    items_auctionRDD.print()
    //total number of items (auctions)
    val totalitems = auctionRDD.map(line => line(0)).count()
    totalitems.print()
    items_auctionRDD.saveAsTextFiles("hdfs://localhost:54310/user/hduser/auctionout/auctiondata")
    ssc.start()
    ssc.awaitTermination()
    //ssc.awaitTerminationOrTimeout(50000)
  }
  
}