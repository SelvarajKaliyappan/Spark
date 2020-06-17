package org.inceptez.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession


object bidsfilestream 
{
  
  case class Auction(auctionid: String, bid: Float, bidtime: Float, bidder: String, bidderrate:Integer, 
        openbid: Float, price: Float, item: String, daystolive: Integer)

  def main(args:Array[String])
  {
      //Old method
      //val sparkConf = new SparkConf().setAppName("textstream").setMaster("local[*]")
      //val sparkcontext = sparkSession.SparkContext(sparkConf)
      // Create the context
      val spark = SparkSession.builder.appName("bidfilesstream").enableHiveSupport.master("local[*]").getOrCreate();
      val sc = spark.sparkContext;
      //val sqlcontext = sparkSession.sqlContext;
      sc.setLogLevel("ERROR")
      
      val ssc = new StreamingContext(sc, Seconds(10))
      
      //val auctionRDD = sparkcontext.textFile("file:///home/hduser/sparkdata/streaming/");
      val auctionRDD = ssc.textFileStream("file:///home/hduser/sparkdata/streaming/")
      // create an RDD of Auction objects
      // change ebay RDD of Auction objects to a DataFrame
      
      import spark.implicits._
      // Foreach rdd function is an iterator on the streaming micro batch rdds as like map function
      auctionRDD.foreachRDD(rdd => {
        if(!rdd.isEmpty)
        {
          val ebay = rdd.map(_.split("~")).map(p => Auction(p(0), p(1).toFloat, p(2).toFloat,p(3),
          p(4).toInt, p(5).toFloat, p(6).toFloat, p(7), p(8).toInt))
      
          val ebaydf = ebay.toDF;
          ebaydf.createOrReplaceTempView("ebayview");
          
          val auctioncnt = spark.sql("select count(1) from ebayview");
          
          print("executing table query\n")
          print("total auctions are\n")
          auctioncnt.show(1,false);
          print("Executing dataframe\n")
          print("total distinct auctions\n")
          val count = ebaydf.select("auctionid").distinct.count
          System.out.println(count);
          print("Executing dataframe\n")
          print("Max bid, sum of price of items are\n")
          import org.apache.spark.sql.functions._
          val grpbid =
          ebaydf.groupBy("item").agg(max("bid").alias("max_bid"),sum("price").alias("sp")).sort($"sp"
          .desc)
          grpbid.show(10,false);
        }
      })
      print("streaming executing in x seconds \n")
      ssc.start()
      ssc.awaitTermination()
  }
}