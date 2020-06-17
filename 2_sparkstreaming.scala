package org.inceptez.spark.hackathon

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object sparkstreaming 
{
  /*
    Hackathon Topic : Spark Core,SQL, Streaming
   	Author					: Selvaraj.K
   	Company 				: Inceptez Technologies
   	StartDate 			: 30-May-2020
   	EndDate					: 03-June-2020o 
   */
  
  case class Chat(id:Integer,chat:String,ctype:String)
  
  def main(args:Array[String])
  {
      //Create SparkSession object here
      val spark = SparkSession.builder().appName("SparkStreamingHackthon2020New").master("local[*]")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
      .config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
      .config("spark.eventLog.dir", "file:////tmp/spark-events")
      .config("spark.eventLog.enabled", "true")
      .enableHiveSupport().getOrCreate();
      
      val sc = spark.sparkContext
      val sqlc = spark.sqlContext
    
      //display only ERROR info.
      sc.setLogLevel("ERROR")
      
      import spark.implicits._
      //import SQL function API.
      import org.apache.spark.sql.functions._
      
      //hadoop file system handling
      val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:54310"),
      sc.hadoopConfiguration)
          
      println("###########################################################################################################")
      println("6. Realtime Streaming")
      println("###########################################################################################################")
      //Usecase - 1. Load the chatdata into a Dataframe using ~ delimiter by pasting in kafka or streaming file source or
      //socket with the seconds of 50.

      val ssc = new StreamingContext(sc, Seconds(10))
     
      println("###########################################################################################################")
      println("Read streaming data using Kafka option")
      println("###########################################################################################################")
      
      val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafkaHackathon",
      "auto.offset.reset" -> "earliest"
      )
      
      //Topic info
      val topics = Array("tk1")
      
      val stream = KafkaUtils.createDirectStream[String, String](ssc,
      PreferConsistent, Subscribe[String, String](topics, kafkaParams) )
      
      val kafkastream = stream.map(record => (record.key, record.value))
      val chatFS = kafkastream.map(rec => rec._2);
      
      /*
      println("#################################################################################################")
      println("Read streaming data using File streaming option")
      println("#################################################################################################")
      
      val chatFS = ssc.textFileStream("file:///home/hduser/sparkdata/streaming/")
      */
      
      /*
      println("#################################################################################################")
      println("Read streaming data using Socket option")
      println("#################################################################################################")
      
      val chatFS = ssc.socketTextStream("localhost", 9999)
      */    
      
      chatFS.foreachRDD(rdd => {
        if(!rdd.isEmpty)
        {
          //Usecase - 2. Define the column names as id,chat,'type' where id is the customer who is chatting with the support
          //agent, chat is the plain text of the chat message, type is the indicator of c means customer or a means agent.
          val chatRDD = rdd.map(x=>x.split("~")).map(y=>Chat(y(0).toInt,y(1),y(2)))
          
          //Usecase - 3. Filter only the records contains 'type' as 'c' (to filter only customer interactions)
          //Usecase - 4. Remove the column 'type' from the above dataframe, hence the resultant dataframe contains 
          //only id and chat and convert to tempview.
      
          val chatDF = chatRDD.filter(x=>x.ctype.toLowerCase() == "c").toDF().drop($"ctype")
        
          //Usecase - 5. Use SQL split function on the 'chat' column with the delimiter as ' ' (space) to tokenize all words for
          /*
            eg. if a chat looks like this (i have internet issue from yesterday) and convert the chat column as array
            type and name it as chat_tokens which has to looks like [i,have,internet,issue,from,yesterday]
            id,chat_tokens
            1,[i,have,internet,issue,from,yesterday]
					*/
          //https://mungingdata.com/apache-spark/arraytype-columns/
  
          val splitChatDF = chatDF.withColumn("chat_tokens", split($"chat","\\ ")).drop("chat")
          println("Show chat sentense in Array format")
          println("---------------------------------------------------------------------------------------------------------------------------")
          splitChatDF.show(10,false) 
          
          //Usecase - 6. Use SQL explode function to pivot the above data created in step 5, then the exploded/pivoted data
          /*    
              looks like below, register this as a tempview.
              id,chat_splits
              1,i
              1,have
              1,internet
              1,issue
              1,from
              1,yesterday
		*/
          val explodeChatDF = splitChatDF.select(col("id"),explode(col("chat_tokens")).as("chat_splits"))
          println("---------------------------------------------------------------------------------------------------------------------------")
          println("Created chat view with pivot option.")
          explodeChatDF.createOrReplaceTempView("chatview")
          sqlc.sql("Select * from chatview").show(10,false)
          
          //Usecase - 8. Load the stopwords from linux file location /home/hduser/stopwordsdir/stopwords (A single column
          //file found in the web that contains values such as (a,above,accross,after,an,as,at) etc. which is used in
          //any English sentences for punctuations or as fillers) into dataframe with the column name stopword and
          //convert to tempview.
          val stopwordsDF = sc.textFile("file:///home/hduser/SPARK_HACKATHON_2020/realtimedataset/stopwords").toDF("stopword")
          println("Created stopwords view with single columns")
          println("---------------------------------------------------------------------------------------------------------------------------")
          stopwordsDF.createOrReplaceTempView("stopwordsview")
          sqlc.sql("Select * from stopwordsview").show(10,false)
          
          //Usecase - 9. Write a left outer join between chat tempview created in step 6 and stopwords tempview created in
          //step 8 and filter all nulls (or) use subquery with not in option to filter all stop words from the actual chat
          //tempview using the stopwords tempview created above.
          val joinChatAndStopwordsDF = sqlc.sql("""select c.id as id, c.chat_splits as chat from chatview as c 
            left outer join stopwordsview as s on c.chat_splits = s.stopword where s.stopword is null""")
          //joinChatAndStopwordsDF.show(10,false)
        
          //Usecase - 10. Load the final result into a hive table should have only result as given below using append option.
          if ( joinChatAndStopwordsDF.count > 0 )
          {
            joinChatAndStopwordsDF.write.mode("append").saveAsTable("custdb.custchatinfo")
            
        	  println("Step-9 dataset successfully stord in HIVE table(custdb.custchatinfo) with append option")
          }
          else
          {
            println("There is no dataset for step-9 to write in to HIVE")
          }
          
          //Usecase - 11. Identify the most recurring keywords used by the customer in all the chats by grouping based on the
          /*
            keywords used with count of keywords. use group by and count functions in the sql
           	for eg.
          	issues,2
          	notworking,2
          	tv,2
          */
          
          joinChatAndStopwordsDF.createOrReplaceTempView("joinedchatview")
          //val recurrChatkeywordDF = sqlc.sql("""select chat as chatkeywords,count(*) as occurance from joinedchatview 
            //group by chat order by 2 desc""")
            
          val recurrChatkeywordDF = sqlc.sql("""with recurrKey as (select chat as chatkeywords,count(*) as occurance,
            dense_rank() over(order by count(*) desc) as rank
            from joinedchatview group by chat ) select chatkeywords,occurance from recurrKey where rank = 1""")
            
          recurrChatkeywordDF.show(10,false)
          
          //Usecase - 12. Store the above result in a json format with column names as chatkeywords, occurance
          if ( recurrChatkeywordDF.count > 0 )
          {
            fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/sparkhack2/output/ssc_step12_JSON"),true)
        
            recurrChatkeywordDF.coalesce(1).write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/sparkhack2/output/ssc_step12_JSON")
        	  println("SSC_Step-12_JSON dataset successfully stord in HDFS location under - /user/hduser/sparkhack2/output/ssc_step12_JSON")
          }
          else
          {
            println("There is no dataset for ssc_Step_12 to write in to HDFS ")
          }
          println("Streaming execution completed successfully \n")  
        }
      })
     println("------------------------------------------------------------------------------------------------------------------------------------")
     println("Streaming execution is starting in every 50 seconds automatically \n")
     println("------------------------------------------------------------------------------------------------------------------------------------")

     ssc.start()
     ssc.awaitTermination() 
  }
}