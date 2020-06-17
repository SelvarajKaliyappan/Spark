package org.inceptez.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object kafkastream 
{
  
  def main(args:Array[String])
  {
    
    val sparkConf = new SparkConf().setAppName("kafkastream").setMaster("local[*]")
    val sparkcontext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkcontext, Seconds(10))
    //ssc.checkpoint("checkpointdir")
    
    val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafkatest1",
    "auto.offset.reset" -> "earliest"
    )
    val topics = Array("tk1")
    
    val stream = KafkaUtils.createDirectStream[String, String](ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
    )
    val kafkastream = stream.map(record => (record.key, record.value))
    val inputStream = kafkastream.map(rec => rec._2);
    val words = inputStream.flatMap(_.split(" "))
    val results = words.map(x => (x, 1L)).reduceByKey(_ + _)
    inputStream.print();
    results.saveAsTextFiles("hdfs://localhost:54310/user/hduser/kafkastreamout/outdir")
    ssc.start()
    ssc.awaitTermination()

  }
  
}