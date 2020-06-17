package org.inceptez.spark.hackathon

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,DateType,StringType}
import org.apache.spark.sql.functions.{concat,col,lit,udf,max,min,current_date,current_timestamp,isnull};

object sparkcoreandsql extends org.inceptez.spark.hackathon.allMethods
{
  /*
    Hackathon Topic : Spark Core,SQL, Streaming
   	Author					: Selvaraj.K
   	Company 				: Inceptez Technologies
   	StartDate 			    : 30-May-2020
   	EndDate					: 03-June-2020
  */
 
   //case class for insureClass
   case class insureClass(IssuerId:Integer,IssuerId2:Integer,BusinessDate:String,StateCode:String,SourceName:String,NetworkName:String,NetworkURL:String,CustNum:Integer,MarketCoverage:String,DentalOnlyPlan:String)
  
   //scala main method - Entry point of the program
   def main(args : Array[String])
   {
      //input files details
      val insureDataFile1 = "hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo1.csv"
      val insureDataFile2 = "hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo2.csv"
      val custStatesData  = "hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv"
      
      //Create SparkSession object here
      val spark = SparkSession.builder().appName("SparkCoreSQLHackthon2020Selva").master("local[*]")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
      .config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
      .config("spark.eventLog.dir", "file:////tmp/spark-events")
      .config("spark.eventLog.enabled", "true")
      .enableHiveSupport().getOrCreate();
      
      val sc = spark.sparkContext
      val sqlc = spark.sqlContext
      
      //Create allMethods object initiation here.
      val objMet = new org.inceptez.spark.hackathon.allMethods
 		  
      //display only ERROR info.
      sc.setLogLevel("ERROR")
      
      //hadoop file system handling
      val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:54310"),
      sc.hadoopConfiguration)
      
      import spark.implicits._
      import org.apache.spark.sql.functions._
    
      println()
      println("##############################################################################################")
      println("1. Data cleaning, cleansing, scrubbing for insure dataset1 - insuranceinfo1.csv")
      println("##############################################################################################")
      println()
      
      //Usecase - 1. Load the file1 (insuranceinfo1.csv) from HDFS using textFile API into an RDD insuredata
      val insureData =  sc.textFile(insureDataFile1)
      
      //Usecase - 2. Remove the header line from the RDD contains column names.
      val headerRow = insureData.first()
      //println(headerRow)
      /*
       col0 - IssuerId
       col1 - IssuerId2
       col2 - BusinessDate
       col3 - StateCode
       col4 - SourceName
       col5 - NetworkName
       col6 - NetworkURL
       col7 - custnum
       col8 - MarketCoverage
       col9 - DentalOnlyPlan
      */
      val filteredInsureData = insureData.filter(row => row != headerRow)
      
      //Usecase - 3. Display the count and show few rows and check whether header is removed.
      println("*************************************************************************************")
      println("Usecase-3. Display the count and show few rows and check whether header is removed.")
      println("*************************************************************************************")
      println("Original insuredata record count is " + insureData.count)
      var filteredInsureDataRowCnt = filteredInsureData.count
      println("Filtered insuredata record count is " + filteredInsureDataRowCnt)
      println("Display top 5 rows from filtered insuredata")
      println("----------------------------------------------------------------------------------------------------------------------------")
      filteredInsureData.take(5).foreach(println)
      
      //Usecase - 4. Remove the blank lines in the rdd.
      /*
      println()
      println("************************************************")
      println("Usecase - 4. Remove the blank lines in the rdd.")
      println("************************************************")
      */
      val rmBlnkInsureData = filteredInsureData.filter(blnkLine => blnkLine.trim().length() > 0)
      /*
      //rmBlnkInsureData.take(5).foreach(println)
      var rmBlnkInsureDataRowCnt = rmBlnkInsureData.count
      println("Removed blank line insuredata record count is " + rmBlnkInsureDataRowCnt)
      
      if( filteredInsureDataRowCnt == rmBlnkInsureDataRowCnt)
      {
        println("There is no blank line records on in insuredata2")
      }
      */
      
      //Usecase - 5. Map and split using ‘,’ delimiter.
       /*
          Hint: To avoid getting incomplete rows truncated, use split(“,”,-1) instead of split(“,”)
          For Eg:
          If you take the rdd.first it should show the output like the below
          Array(21989, 21989, 2019-10-01, AK, HIOS, ODS Premier,
          https://www.modahealth.com/ProviderSearch/faces/webpages/home.xhtml?dn=ods, 13, "", "")
          Output should not be like given below
          Array(21989, 21989, 2019-10-01, AK, HIOS, ODS Premier,
          https://www.modahealth.com/ProviderSearch/faces/webpages/home.xhtml?dn=ods, 13)
      println()
      println("***********************************************")
      println("Usecase - 5. Map and split using ‘,’ delimiter.")
      println("***********************************************")
      */
      val deliInsureData = rmBlnkInsureData.map(row => row.split(",",-1))
      /*
      println("Display first rows from delimited insuredata")
      println("-----------------------------------------------")
      println(deliInsureData.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9) )).first())
      */
      
      /*
        Usecase - 6. Filter number of fields are equal to 10 columns only - analyze why we are doing this and provide your view here.
			  Reason(s) are below
        1) Only good data(has 10 columns) will flow for process. 
        2) Less than 10 column's data will be rejected and send for review. 
      */ 
      val filColInsureData = deliInsureData.filter(row => row.length == 10)
      //println("No.of rows with 10 column count is " + filColInsureData.count)
      
      //Usecase - 7. Add case class namely insureclass with the field names used as per the header record in the file and apply to the above data to create schemaed RDD.
      val schemaInsureData = filColInsureData.map(x=>insureClass(x(0).toInt,x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7).toInt,x(8),x(9)))
      //schemaInsureData.take(10).foreach(println)
      
      //Usecase - 8. Take the count of the RDD created in step 7 and step 1 and print how many rows are removed in the cleanup process of removing fields does not equals 10.
      println()
      println("*******************************************************************************************************************************************************************")
      println("Usecase - 8. Take the count of the RDD created in step 7 and step 1 and print how many rows are removed in the cleanup process of removing fields does not equals 10.")
      println("*******************************************************************************************************************************************************************")
      var schemaInsureDataRddCnt = schemaInsureData.count()
      var insureDataRddCnt = insureData.count()
      println("Step 1st insure raw dataset1 row count is - " +insureDataRddCnt)
      println("Step 7th Scheamed dataset row count is - " +schemaInsureDataRddCnt)
      println("No of rows removed between raw dataset and schemaed dataset is - " + (insureDataRddCnt - schemaInsureDataRddCnt) )
      
      //Usecase - 9. Create another RDD namely rejectdata and store the row that does not equals 10 columns.
      val rejectedInsureData = deliInsureData.filter(row => row.length < 10)
      /*
      if (rejectedInsureData.count() > 0)
      {
        println("Rejected records count for insuredata1 is - " + rejectedInsureData.count)
      	rejectedInsureData.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5))).foreach(println)
      }
      else
      {
        println("There is no rejected records for less than 10 columns in insuredata1")
      }
      */
      
      println()
      println("##############################################################################################")
      println("Data cleaning, cleansing, scrubbing for insure dataset2 - insuranceinfo2.csv")
      println("##############################################################################################")
      println()
      
      //Usecase - 10. Load the file2 (insuranceinfo1.csv) from HDFS using textFile API into an RDD insuredata2
      val insureData2 =  sc.textFile(insureDataFile2)
      //println(insureData2.count)
      
      //Usecase - 2.1. Remove the header line from the RDD contains column names.
      val headerRow2 = insureData2.first()
      //println(headerRow2)
      /*
       IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan
      */
      val filteredInsureData2 = insureData2.filter(row => row != headerRow2)
      //println(filteredInsureData2.count)
      
      //Usecase - 3.1 Display the count and show few rows and check whether header is removed.
      println("*************************************************************************************")
      println("Usecase-3.1 Display the count and show few rows and check whether header is removed.")
      println("*************************************************************************************")
      println("Original insuredata2 record count is " + insureData2.count)
      var filteredInsureDataRowCnt2 = filteredInsureData2.count
      println("Filtered insuredata2 record count is " + filteredInsureDataRowCnt2)
      println("Display top 5 rows from filtered insuredata2")
      println("----------------------------------------------------------------------------------------------------------------------------")
      filteredInsureData2.take(5).foreach(println)
      
      /*
      //Usecase - 4.1 Remove the blank lines in the rdd.
      println()
      println("************************************************")
      println("Usecase - 4.1 Remove the blank lines in the rdd.")
      println("************************************************")
      */
      val rmBlnkInsureData2 = filteredInsureData2.filter(blnkLine => blnkLine.trim().length() > 0)
      /*
      //rmBlnkInsureData2.take(5).foreach(println)
      var rmBlnkInsureDataRowCnt2 = rmBlnkInsureData2.count
      println("Removed blank line insuredata2 record count is " + rmBlnkInsureDataRowCnt2)
      
      if( filteredInsureDataRowCnt2 == rmBlnkInsureDataRowCnt2)
      {
        println("There is no blank line records on in insuredata2")
      }
      */
      
      /*
      //Usecase - 5.1 Map and split using ‘,’ delimiter.
          Hint: To avoid getting incomplete rows truncated, use split(“,”,-1) instead of split(“,”)
          For Eg:
          If you take the rdd.first it should show the output like the below
          Array(21989, 21989, 2019-10-01, AK, HIOS, ODS Premier,
          https://www.modahealth.com/ProviderSearch/faces/webpages/home.xhtml?dn=ods, 13, "", "")
          Output should not be like given below
          Array(21989, 21989, 2019-10-01, AK, HIOS, ODS Premier,
          https://www.modahealth.com/ProviderSearch/faces/webpages/home.xhtml?dn=ods, 13)
      println()
      println("***********************************************")
      println("Usecase - 5.1 Map and split using ‘,’ delimiter.")
      println("***********************************************")
       */
      val deliInsureData2 = rmBlnkInsureData2.map(row => row.split(",",-1))
      /*
      //println(deliInsureData2.count)
      println("Display first rows from delimited insuredata2")
      println("-----------------------------------------------")
      */
      //Remove empty IssuerId column data
      val nonEmptyIssuerId = deliInsureData2.filter(x => x(0).length > 0)
      //println(nonEmptyIssuerId.count)
      //println(nonEmptyIssuerId.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9) ))first())
          
      /*
        Usecase - 6.1 Filter number of fields are equal to 10 columns only - analyze why we are doing this and provide your view here.
			  Reason(s) are below
        1) Only good data(has 10 columns) will flow for process. 
        2) Less than 10 column's data will be rejected and send for review. 
      */ 
      val filColInsureData2 = nonEmptyIssuerId.filter(row => row.length == 10)
      //println("No.of rows with 10 column count is " + filColInsureData2.count)
            
      //Usecase - 7.1 Add case class namely insureclass with the field names used as per the header record in the file and apply to the above data to create schemaed RDD.
      val schemaInsureData2 = filColInsureData2.map(x=>insureClass(x(0).toInt,x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7).toInt,x(8),x(9)))
      //schemaInsureData2.take(10).foreach(println)
      //println(schemaInsureData2.count)
            
      //Usecase - 8.1 Take the count of the RDD created in step 7.1 and step 10 and print how many rows are removed in the cleanup process of removing fields does not equals 10.
      println()
      println("*******************************************************************************************************************************************************************")
      println("Usecase - 8.1 Take the count of the RDD created in step 7 and step 1 and print how many rows are removed in the cleanup process of removing fields does not equals 10.")
      println("*******************************************************************************************************************************************************************")
      var schemaInsureDataRddCnt2 = schemaInsureData2.count()
      var insureDataRddCnt2 = insureData2.count()
      println("Step 1st insure raw dataset2 row count is - " +insureDataRddCnt2)
      println("Step 7.1th Scheamed dataset2 row count is - " +schemaInsureDataRddCnt2)
      println("No of rows removed between raw dataset2 and schemaed dataset2 is - " + (insureDataRddCnt2 - schemaInsureDataRddCnt2) )
      
      //Usecase - 9.1 Create another RDD namely rejectdata and store the row that does not equals 10 columns.
      val rejectedInsureData2 = deliInsureData2.filter(row => row.length < 10)
      
      /*
      if (rejectedInsureData2.count() > 0)
      {
        println("Rejected records count for insuredata2 is - " + rejectedInsureData2.count)
        rejectedInsureData2.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5))).foreach(println)
      }
      else
      {
        println("There is no rejected records for less than 10 columns in insuredata2")
      }
      */
      
      println()
      println("##############################################################################################")
      println("2. Data merging, Deduplication, Performance Tuning & Persistance")
      println("##############################################################################################")
      println()
      
      //Usecase - 12. Merge the both header removed RDDs derived in steps 7 and 11 into an RDD namely insuredatamerged
      val insureDataMerged = schemaInsureData.union(schemaInsureData2)
      
      //Usecase - 13. Cache it either to memory or any other persistence levels you want, display only first few rows
      insureDataMerged.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2)
      
      println("**************************************************************************************************************")
      println("Usecase 13. Cache it either to memory or any other persistence levels you want, display only first few rows")
      println("**************************************************************************************************************")
      insureDataMerged.take(5).foreach(println)

      //Usecase - 14. Calculate the count of rdds created in step 7+11 and rdd in step 12, check whether they are matching.
      println()
      println("*****************************************************************************************************************")
      println("Usecase 14. Calculate the count of rdds created in step 7+11 and rdd in step 12, check whether they are matching.")
      println("*****************************************************************************************************************")
      
      println("Step-7 dataset count is - " +schemaInsureData.count)
      println("Step-11 dataset count is - " +schemaInsureData2.count)
      println("Merged both insure dataset count - " +insureDataMerged.count)
      
      if ( insureDataMerged.count == (schemaInsureData.count + schemaInsureData2.count) )
      {
        println("Rdd created in step 7th & 11th are matched with step 12th")
      }
      else
      {
        println("Rdd created in step 7th & 11th are NOT matched with step 12th")    
      }
      
      //Usecase - 15. Remove duplicates from this merged RDD created in step 12 and print how many duplicate rows are there.
      println()
      println("*****************************************************************************************************************")
      println("15. Remove duplicates from this merged RDD created in step 12 and print how many duplicate rows are there.")
      println("*****************************************************************************************************************")
      val dedupInsureData = insureDataMerged.distinct()
      println("Deduped insure dataset count is - " +dedupInsureData.count)
      println("Duplicate rows from merged insure dataset is - " +(insureDataMerged.count - dedupInsureData.count) )

      //Usecase - 16. Increase the number of partitions to 8 and name it as insuredatarepart.
      val insuredatarepart = dedupInsureData.repartition(8)
      
      //Usecase - 17. Split the above RDD using the businessdate field into rdd_20191001 and rdd_20191002 
      //based on the BusinessDate of 2019-10-01 and 2019-10-02 respectively
      val rdd_20191001 = insuredatarepart.filter(x => (x.BusinessDate == "01-10-2019" || x.BusinessDate == "2019-10-01"))
      //println("20191001 rdd count is " +rdd_20191001.count)
      val rdd_20191002 = insuredatarepart.filter(y => (y.BusinessDate == "02-10-2019" || y.BusinessDate == "2019-10-02"))
      //println("20191002 rdd count is " +rdd_20191002.count)
      
      //Usecase - 18. Store the RDDs created in step 9, 12, 17 into HDFS locations.
      println()
      println("********************************************************************")
      println("18. Store the RDDs created in step 9, 12, 17 into HDFS locations.")
      println("********************************************************************")
      
      //merge 2 different rejected dataset into one.
      val rejectedInsureDataset = rejectedInsureData.union(rejectedInsureData2)
      println("Rejected merged dataset count is - " +rejectedInsureDataset.count)
      println("----------------------------------------------------------------------------------------------------------------------------")
       
      //step-9 dataset
      if ( rejectedInsureDataset.count > 0 )
      {
        val rejectedInsureRDD = rejectedInsureDataset.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5)))

        fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/sparkhack2/output/step9"),true)
        
        rejectedInsureRDD.coalesce(1).saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/output/step9/")
    	  
        println("Step-9 dataset successfully stord in HDFS location under - /user/hduser/sparkhack2/output/step9")
      }
      else
      {
        println("There is no dataset for Step-9 to write in to HDFS ")
      }
      
      //step-12 dataset
      if ( insureDataMerged.count > 0 )
      {
        fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/sparkhack2/output/step12"),true)
        
        insureDataMerged.map(x=>x.productIterator.mkString("|")).coalesce(1).saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/output/step12/")
        
        println("Step-12 dataset successfully stord in HDFS location under - /user/hduser/sparkhack2/output/step12")
        
      }
      else
      {
        println("There is no dataset for Step-12 to write in to HDFS ")
      }
      
      //step-17 dataset
      if( rdd_20191001.count > 0 )
      {
        fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/sparkhack2/output/step17_1"),true)
        
        rdd_20191001.map(x=>x.productIterator.mkString("|")).coalesce(1).saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/output/step17_1")
        
        println("Step-17(1) dataset successfully stord in HDFS location under - /user/hduser/sparkhack2/output/step17_1")
      }
      else
      {
        println("There is no dataset for Step-17(1) to write in to HDFS ")
      }
      
      if( rdd_20191002.count > 0 )
      {
        fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/sparkhack2/output/step17_2"),true)
        
        rdd_20191002.map(x=>x.productIterator.mkString("|")).coalesce(1).saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/output/step17_2")
        
        println("Step-17(2) dataset successfully stord in HDFS location under - /user/hduser/sparkhack2/output/step17_2")
      }
      else
      {
        println("There is no dataset for Step-17(2) to write in to HDFS ") 
      }  
     
      println()
      
      //Usecase - 19. Convert the RDD created in step 16 above into Dataframe namely insuredaterepartdf
      //applying the structtype created in the step 20 given in the next usecase.
      //Hint: Think of converting df to rdd then use createdataframe to apply schema some thing like this..
      //Eg: createDataframe(df19.rdd,structuretype)
      
      //Usecase - 20. Create structuretype for all the columns as per the insuranceinfo1.csv.
      //Hint: Do it carefully without making typo mistakes. Fields issuerid, issuerid2 should be of
      //IntegerType, businessDate should be DateType and all other fields are StringType, ensure to import sql.types library.

      //import sqlc.implicits._
      val insuredatarepartdf = insuredatarepart.toDF()
      
      val insureDataSchema = StructType(Array(StructField("IssuerId", IntegerType,true),
      StructField("IssuerId2",IntegerType,true),StructField("BusinessDate",DateType,true),
      StructField("StateCode",StringType,true),StructField("SourceName",StringType,true),
      StructField("NetworkName",StringType,true),StructField("NetworkURL",StringType,true),
      StructField("CustNum",StringType,true),StructField("MarketCoverage",StringType,true),
      StructField("DentalOnlyPlan",StringType,true)))
   
      val insure1df = sqlc.createDataFrame(insuredatarepartdf.rdd,insureDataSchema)
            
      println()
      println("##############################################################################################")
      println("3. DataFrames operations")
      println("##############################################################################################")
      println()
      
      //Usecase - 21. Create dataset using the csv module with option to escape ‘,’ accessing the
      //insuranceinfo1.csv and insuranceinfo2.csv files, apply the schema of the structure type created in the step 20.
      val insureDatadf = spark.read.option("header",true).option("escape",",").option("dateFormat", "dd-MM-yy")
        .schema(insureDataSchema).csv(insureDataFile1,insureDataFile2)
      
      //println("InsureInfo row count - " +insureDatadf.count)
      //insureDatadf.show(5,false)
      
      //insureDatadf.createOrReplaceTempView("insurevw")
      //sqlc.sql("select distinct businessdate as BusinessDate from insurevw").show()
    
      //Usecase - 22. Apply the below DSL functions in the DFs created in step 21.
          //a. Rename the fields StateCode and SourceName as stcd and srcnm respectively.
          //b. Concat IssuerId,IssuerId2 as issueridcomposite and make it as a new field
          //Hint : Cast to string and concat.
          //c. Remove DentalOnlyPlan column
          //d. Add columns that should show the current system date and timestamp with the fields name of sysdt and systs respectively.
      
      val renamedInsureDataDF = insureDatadf.withColumnRenamed("StateCode", "stcd")
      .withColumnRenamed("SourceName", "srcnm")
      .withColumn("issueridcomposite", concat($"IssuerId",lit(""),$"IssuerId2"))
      .withColumn("sysdt",current_date()).withColumn("systs", current_timestamp())
      .drop("DentalOnlyPlan")
      
      //println("Renamed InsureInfo row count - " +renamedInsureDataDF.count)
      //renamedInsureDataDF.show(5,false)
      
      //Usecase - 23. Remove the rows contains null in any one of the field and count the number of rows
      //which contains all columns with some value. Hint: Use drop.na options
      val removeNullInsureDatadf = renamedInsureDataDF.na.drop()
      //println("Count after removed the null values in any of the column in both insure data set is - " + removeNullInsureDatadf.count)
      //removeNullInsureDatadf.show(5,false)
      
      //Usecase - 24. Created method in seperate class.
      //var str = objMet.remspecialchar("This - text ! has \\ /allot # of % special % characters")
 		  //println(str)
 		  //str = objMet.remspecialchar("Pathway - 2X (with dental)")
 		  //println(str)

      //Usecase - 25. Import the package, instantiate the class and register the method generated in step 24 as a udf for invoking in the DSL function.
      val rmSpecialChar = udf(objMet.remspecialchar _)
 		  
      //Usecase - 26. Call the above udf in the DSL by passing NetworkName column as an argument to get the special characters removed DF.
      val removeSpciCharInsureDataDF = removeNullInsureDatadf.select($"IssuerId",$"IssuerId2",$"BusinessDate",$"stcd",$"srcnm",rmSpecialChar($"NetworkName").alias("NetworkName"),$"NetworkURL",$"CustNum",$"MarketCoverage",$"issueridcomposite",$"sysdt",$"systs")
      //removeSpciCharInsureDataDF.select(rmSpecialChar($"NetworkName")).show(5,false)
      
      val tempDF = removeSpciCharInsureDataDF.createOrReplaceTempView("tempvw")
      sqlc.sql("select distinct BusinessDate from tempvw").show()
      
      //Usecase - 27. Save the DF generated in step 26 in JSON into HDFS with overwrite option.
      println("Writing insure dataset into HDFS as JSON format")
      println("--------------------------------------------------------------------------------------------------------------------------")
     
      //step-27 dataset
      if ( removeSpciCharInsureDataDF.count > 0 )
      {
        fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/sparkhack2/output/step27_JSON"),true)
        
        removeSpciCharInsureDataDF.write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/sparkhack2/output/step27_JSON")
    	  
        println("Step-27_JSON dataset successfully stord in HDFS location under - /user/hduser/sparkhack2/output/step27_JSON")
      }
      else
      {
        println("There is no dataset for Step_27 to write in to HDFS ")
      }
       
      //Usecase - 28. Save the DF generated in step 26 into CSV format with header name as per the DF and delimited by ~ into HDFS with overwrite option.
      //step-28 dataset
      if ( removeSpciCharInsureDataDF.count > 0 )
      {
        fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/sparkhack2/output/step28_CSV"),true)
        
        removeSpciCharInsureDataDF.write.mode("overwrite").option("header", "true").option("delimiter", "~").
        csv("hdfs://localhost:54310/user/hduser/sparkhack2/output/step28_CSV")
        
        println("Step-28_CSV dataset successfully stord in HDFS location under - /user/hduser/sparkhack2/output/step28_CSV")
      }
      else
      {
        println("There is no dataset for Step_28 to write in to HDFS ")
      }
      
      println()
   
      //Usecase - 29. Save the DF generated in step 26 into hive external table and append the data without overwriting it.
      //step-29 dataset
      if ( removeSpciCharInsureDataDF.count > 0 )
      {
        sqlc.sql("""Create external table if not exists insurance.insureinfo(issuerid int,issuerid2 int,
          businessdate date,stcd string,srcnm string,networkname string,networkurl string,custnum string,
          marketcoverage string,issueridcomposite string,sysdt date,systs timestamp) 
          row format delimited fields terminated by ',' stored as textfile 
          location '/user/hive/warehouse/insurance.db/insureinfo'""")
        
        removeSpciCharInsureDataDF.write.mode("append").saveAsTable("insurance.insureinfo")
        
    	  println("Step-29 dataset successfully stord in HIVE  external table with append option")
      }
      else
      {
        println("There is no dataset for Step_29 to write in to HIVE")
      }

      println()
      println("##############################################################################################")
      println("4. Tale of handling RDDs, DFs and TempViews")
      println("##############################################################################################")
      println()
      /*
     		 	Loading RDDs, split RDDs, Load DFs, Split DFs, Load Views, Split Views, write UDF, register to
					use in Spark SQL, Transform, Aggregate, store in disk/DB
      */
      
      //Usecase - 30. Load the file3 (custs_states.csv) from the HDFS location, using textfile API in an RDD
      //custstates, this file contains 2 type of data one with 5 columns contains customer
      //master info and other data with statecode and description of 2 columns.
      val custStatesDataRdd =  sc.textFile(custStatesData)  
      //println("Customer States raw file row count is - " + custStatesDataRdd.count())
      println("----------------------------------------------------------------------------------------------------------------------------")
      
      //Usecase - 31. Split the above data into 2 RDDs, first RDD namely custfilter should be loaded only with
      //5 columns data and second RDD namely statesfilter should be only loaded with 2 columns data.
      val custFilterRdd = custStatesDataRdd.map(row => row.split(",")).filter(flrow => flrow.length == 5)
      //println("Customer data row count is - " + custFilterRdd.count())
      
      val statesFilterRdd = custStatesDataRdd.map(row => row.split(",")).filter(flrow => flrow.length == 2)
      //println("States data row count is - " + statesFilterRdd.count())
      
      val rejectedFilterRdd = custStatesDataRdd.map(row => row.split(",")).filter(flrow => (flrow.length > 2 & flrow.length < 5) )
      //println("Rejected data row count is - " + rejectedFilterRdd.count())
      
      //Usecase - 32. Load the file3 (custs_states.csv) from the HDFS location, using CSV Module in a DF
      //custstatesdf, this file contains 2 type of data one with 5 columns contains customer
      //master info and other data with statecode and description of 2 columns.
      val custStatesDatadf = spark.read.option("delimiter", ",").csv(custStatesData)
      //println("Customer States raw file row count is - " + custStatesDatadf.count())
      //custStatesDatadf.show(5,false)
      
      //Usecase - 33. Split the above data into 2 DFs, first DF namely custfilterdf should be loaded only with 5
      //columns data and second DF namely statesfilterdf should be only loaded with 2 columns data.
      val custFilterdf = custStatesDatadf.filter(col("_c0").isNotNull).filter(col("_c1").isNotNull)
      .filter(col("_c2").isNotNull).filter(col("_c3").isNotNull).filter(col("_c4").isNotNull)
   
      val renamedcustFilterdf = custFilterdf.withColumnRenamed("_c0", "CustId").withColumnRenamed("_c1", "FirstName")
      .withColumnRenamed("_c2", "LastName").withColumnRenamed("_c3", "Age").withColumnRenamed("_c4", "Profession")
     
      //println("Customer data row count is - " + custFilterdf.count())
      //custFilterdf.show(5,false)
      
      val stateFilterdf = custStatesDatadf.filter(col("_c0").isNotNull).filter(col("_c1").isNotNull)
      .filter(col("_c2").isNull).filter(col("_c3").isNull).filter(col("_c4").isNull)
      .drop("_c2","_c3","_c4")
      
      val renamedstateFilterdf = stateFilterdf.withColumnRenamed("_c0", "StateId").withColumnRenamed("_c1", "StateName")
      //println("Customer data row count is - " + stateFilterdf.count())
      //stateFilterdf.show(5,false)
      
      //Usecase - 34. Register the above DFs as temporary views as custview and statesview.
      renamedcustFilterdf.createOrReplaceTempView("custview")
      renamedstateFilterdf.createOrReplaceTempView("statesview")
      
      //sqlc.sql("Select * from custview").show(5,false)
      //sqlc.sql("Select * from statesview").show(5,false)
      
      //Usecase - 35. Register the DF generated in step 22 as a tempview namely insureview
      renamedInsureDataDF.createOrReplaceTempView("insureview")     
      
      //sqlc.sql("select * from insureview").show(5,false)
      
      //Usecase - 36. Import the package, instantiate the class and Register the method created in step 24 in
      //the name of remspecialcharudf using spark udf registration.
      spark.udf.register("remspecialcharudf",objMet.remspecialchar _)
      spark.udf.register("left",objMet.left _)
      
      //Usecase - 37. Write an SQL query with the below processing
      /*
          Hint: Try doing the below , step by step, skip if you can’t do it, later try.
          
          a. Pass NetworkName to remspecialcharudf and get the new column called
          cleannetworkname
          
          b. Add current date, current timestamp fields as curdt and curts.
          
          c. Extract the year and month from the businessdate field and get it as 2 new fields
          called yr,mth respectively.
          
          d. Extract from the protocol either http/https from the NetworkURL column, if no
          protocol found then display noprotocol. For Eg: if http://www2.dentemax.com/
          then show http if www.bridgespanhealth.com then show as noprotocol store in a column called protocol.
          
          e. Display all the columns from insureview including the columns derived from
          above a, b, c, d steps with statedesc column from statesview with age,profession
          column from custview . Do an Inner Join of insureview with statesview using
          stcd=stated and join insureview with custview using custnum=custid.
      */
      //insureinfo
      val insureinfosql = sqlc.sql("""select distinct IssuerId,IssuerId2,BusinessDate,StCd,SrcNm,remspecialcharudf(NetworkName) as CleanNetworkName,
        NetworkURL,CustNum,MarketCoverage,Issueridcomposite,current_date() as CurDt,current_timestamp() as CurTs, 
        date_format(BusinessDate,'MMM') as Mth, date_format(BusinessDate,'yyyy') as Yr,
        case when (left(NetworkURL,4) = 'http' or left(NetworkURL,5) = 'https') then 'http' else 'noprotocol' end as Protocol,
        S.statename as Statename, C.age as Age, C.profession as Profession
        from insureview as I Inner Join statesview as S on I.stcd = Stateid
        Inner Join custview as C on I.custnum = C.custid""")
        
        //insureinfosql.createOrReplaceTempView("temp1vw")
        //sqlc.sql("select distinct protocol from temp1vw").show()
        
      //Usecase - 38. Store the above selected Dataframe in Parquet formats in a HDFS location as a single file.
      //step-38 dataset
      if ( insureinfosql.count > 0 )
      {
        fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/sparkhack2/output/step38_Parquet"),true)
        
        insureinfosql.coalesce(1).write.parquet("hdfs://localhost:54310/user/hduser/sparkhack2/output/step38_Parquet")
        
        println("Step-38_Parquet dataset successfully stord in HDFS location under - /user/hduser/sparkhack2/output/step38_Parquet")
      }
      else
      {
        println("There is no dataset for Step_38 to write in to HDFS ")
      }

      //Usecase - 39. Very very important interview question : 
      /*    
          Write an SQL query to identify average age,
          count group by statedesc, protocol, profession including a seqno column added which
          should have running sequence number partitioned based on protocol and ordered
          based on count descending.
          For eg.
          Seqno,Avgage,count,statedesc,protocol,profession
          1,48.4,10000, Alabama,http,Pilot
          2,72.3,300, Colorado,http,Economist
          1,48.4,3000, Atlanta,https,Health worker
          2,72.3,2000, New Jersey,https,Economist
			*/
      insureinfosql.createOrReplaceTempView("insureinfosql")
      
      /*
      sqlc.sql("""Select row_number() over(partition by protocol order by count(age) desc) as Seqno,
      cast(avg(Age) as decimal(4,1)) as AvgAge,count(age) as Count, StateName,Protocol,Profession 
      from insureinfosql group by StateName,Protocol,Profession""").show(500,false)
        
      sqlc.sql("""Select row_number() over(partition by protocol order by count(*) desc) as Seqno,
      cast(avg(Age) as decimal(4,1)) as AvgAge,count(*) as Count, StateName,Protocol,Profession 
      from insureinfosql group by StateName,Protocol,Profession""").show(500,false)
      */
      
      val aggInsuresql = sqlc.sql("""Select row_number() over(partition by protocol order by count(age) desc) as Seqno,
        cast(avg(Age) as decimal(4,1)) as AvgAge,count(age) as Count, StateName,Protocol,Profession 
        from insureinfosql group by StateName,Protocol,Profession""")
        
      //Usecase - 40. Store the DF generated in step 39 into MYSQL table insureaggregated.
      println("-------------------------------------------------------------------------------------------------------------------------")
      val prop=new java.util.Properties();
      prop.put("user", "root")
      prop.put("password", System.getenv("mysqlp"))
      
      if ( insureinfosql.count > 0 )
      {
        insureinfosql.write.mode("overwrite").jdbc("jdbc:mysql://localhost/custdb","insureaggregated",prop)
        println("Step-40 dataset successfully stord in MySQL custdb")
      }
      else
      {
        println("There is no dataset for Step_40 to write in to MySQL ")
      }
      
      //clear the cache which created in step-13.
      insureDataMerged.unpersist()
      
      println("------------------------------------------------------------------------------------------------------------------------")
  }
  
}