package org.inceptez.sparkusecases

object sparkcoreobj 
{
  //create case class for this video dataset
   case class youtubeVideo(id:String, duration:String, bitrate:String, bitrate_video:String, height:String, width:String, frame_rate:String, frame_rate_est:String, codec:String,category:String, url:String)
    
  def main(args : Array[String])
  {
    //create spark conf and context object here.
    val sconf = new org.apache.spark.SparkConf().setAppName("Selvaraj").setMaster("local[2]")
    val izsc = new org.apache.spark.SparkContext(sconf)
   
    //welcome message
    println("Welcome to spark core use case work out !")
    
    izsc.setLogLevel("ERROR")
    
    //1. create a hdfs filerdd from the file in a location /user/hduser/videolog/youtube_videos.tsv
    val raw_videoFileRdd = izsc.textFile("hdfs://localhost:54310/user/hduser/videolog/youtube_videos.tsv")
        
    //11. Create an rdd called filerdd4part from filerdd created in step1 by increasing the number of
    //partitions to 4 (Execute this step anywhere in the code where ever appropriate)
    val partitionBy4Rdd = raw_videoFileRdd.repartition(4)
    
    //12. Persist the filerdd4part data into memory and disk with replica of 2, (Execute this step anywhere
    //in the code where ever appropriate)
    partitionBy4Rdd.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2)
    
    //2. split the rows using tab ("\t") delimiter
    val ds_splitByRdd = partitionBy4Rdd.map(x=>x.split("\t"))
    //println("Check no of rows before skip header " +ds_splitByRdd.count())
        
    //display only header
    //ds_splitByRdd.map(x=>for(i <- x){println(i)}).take(1)
    /*
       	column(0)  - id
       	column(1)  - duration
        column(2)  - bitrate
        column(3)  - bitrate(video)
        column(4)  - height
        column(5)  - width
        column(6)  - frame rate
        column(7)  - frame rate(est.)
        column(8)  - codec
        column(9)  - category
        column(10) - url
     */
    
    //3. Remove the header record by filtering the first column value does not contains "id" into an rdd splitrdd
    val ds_filteredByRdd = ds_splitByRdd.filter(x => !x.contains("id"))
    //println("Check no of rows after skip header " +ds_filterdByRdd.count())
    
    //Apply Case class in to video RDD file.
    val filteredVideoRdd = ds_filteredByRdd.map(v=>youtubeVideo(v(0),v(1),v(2),v(3),v(4),v(5),v(6),v(7),v(8),v(9),v(10)))
    
    //12. Persist the filerdd4part data into memory and disk with replica of 2, (Execute this step anywhere
    //in the code where ever appropriate)
    filteredVideoRdd.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2)

    //4. display only first 10 rows in the screen from splitrdd.
    println("**********************************************************************************************")
    println("Usecase 4 - Display only first 10 rows in the screen")
    filteredVideoRdd.take(10).foreach(println)
    
    //5. filter only Music category data from splitrdd into an rdd called music
    val ds_musicRdd =  filteredVideoRdd.filter(x => x.category.toLowerCase == "music")
    println("**********************************************************************************************")
    println("Usecase 5 - Display only first 5 music category")
    ds_musicRdd.take(5).foreach(println)
    
    //6. filter only duration>100 data from splitrdd into an rdd called longdur
    val ds_longdurRdd =  filteredVideoRdd.filter(x => x.duration.toInt > 100)
    println("**********************************************************************************************")
    println("Usecase 6 - Filter only duration>100")
    ds_longdurRdd.take(5).foreach(println)
    
    //7. Union music and longdur rdd and find only distinct records into an rdd music_longdur
    val ds_music_longdurRdd = ds_musicRdd.union(ds_longdurRdd)
    val ds_dist_music_longdurRdd = ds_music_longdurRdd.distinct
    
    /* To check count of each dataset
    println("No of rows in music dataset " +ds_musicRdd.count())
    println("No of rows in long duration dataset " +ds_longdurRdd.count())
    println("No rows in music & long duration dataset " +ds_music_longdurRdd.count())
    println("Distinct rows in music & long duration dataset " +ds_dist_music_longdurRdd.count())
    */
    
    //8. Select only id, duration, codec and category by re ordering the fields like id,category,codec,duration into an rdd mapcolsrdd
    val ds_mapcolsRdd = ds_dist_music_longdurRdd.map(x=>(x.id,x.category,x.codec,x.duration))
    println("**********************************************************************************************")
    println("Usecase 8 - Select only id, duration, codec and category by re ordering the fields like id,category,codec,duration")
    ds_mapcolsRdd.take(5).foreach(println)
    
    //9.Select only duration from mapcolsrdd and find max duration by using max fuction.
    val ds_durationRdd = ds_mapcolsRdd.map(x=>x._4)
    println("**********************************************************************************************")
    println("Usecase 9 - Select Min and Max duration")
    println("Max duration " +ds_durationRdd.max, "the Min duration "+ds_durationRdd.min)
    
    //10. Select only codec from mapcolsrdd, convert to upper case and print distinct of it in the screen.
    val ds_codecRdd = ds_mapcolsRdd.map(x=>x._3.toUpperCase())
    println("**********************************************************************************************")
    println("Usecase 10 - Select distinct codec")
    ds_codecRdd.distinct.foreach(println)
    
    //13. Calculate and print the overall total, max, min duration for Comedy category
    val ds_comedyRdd =  filteredVideoRdd.filter(x => x.category.toLowerCase == "comedy")
    println("**********************************************************************************************")
    println("Usecase 13 - Calculate and print the overall total, max, min duration for Comedy category")
    println("Total Comedy category video count is " +ds_comedyRdd.count())
    val ds_comedyDurationRdd = ds_comedyRdd.map { x => x.duration }
    println("Comdedy Category Max duration is " +ds_comedyDurationRdd.max, "Comdedy Category Min duration is "+ds_comedyDurationRdd.min)
    
    //14. Print the codec wise count and minimum duration not by using min function.
    val ds_codecWiceRdd = filteredVideoRdd.map(x => (x.codec,x.duration))
    println("**********************************************************************************************")
    println("Usecase 14 - Print the codec wise count and minimum duration not by using min function")
    println("Codec wise count are below ")
    ds_codecWiceRdd.map(x=>x._1).countByValue().foreach(println)
    println("Min duration not using min function "+ds_codecWiceRdd.sortBy(s=>s._2,true).first())
   
    //15. Print the distinct category of videos
    val ds_distCategoryRdd = filteredVideoRdd.map(x => x.category)
    println("**********************************************************************************************")
    println("Usecase 15 - Print the distinct category of videos")
    ds_distCategoryRdd.distinct.foreach(println)
   
    //16. Print only the id, duration, height and width sorted by duration.
    //Sory by duration descending.
    val ds_printRdd = filteredVideoRdd.map(x => (x.id,x.duration,x.height,x.width)).sortBy(s=>s._2,false)
    println("**********************************************************************************************")
    println("Usecase 16 - Print only the id, duration, height and width sorted by duration")
    ds_printRdd.take(10).foreach(println)
        
    //17. Merge the rdds generated in step5 and step13.
    val ds_mergeCat = ds_musicRdd ++ ds_comedyRdd
    println("**********************************************************************************************")
    println("Usecase 17 - Merged Music and Comedy category")
    println("Merged Music and Comedy category total are "+ds_mergeCat.count())
    
    //18. Store the step 17 result in a hdfs location in a single file with data delimited as |
    //delete the directory if already exists
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:54310"),
    izsc.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/sparkcoreusecase"),true)

    //save the data into HDFS
    ds_mergeCat.map(x=>x.productIterator.mkString("|")).coalesce(1).saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkcoreusecase/")
    
    println("**********************************************************************************************")
    println("Usecase 18 - Successfully, stored the Merged Music and Comedy category into HDFS")
        
    
    //clear the cache/persist the rdd
    filteredVideoRdd.unpersist()
    /* New use case */
    
    //8. Using the split rdd created in the step 2 below, create another rdd called hashmaskrdd should
    //return (id,maskedcategory) by passing id,hashMask(category) in the map method and print only
    //first 20 tuples using take.For Eg like this: map(x=>(x(0),hashMask(x(11))))
    val objmask = new com.inceptez.datasecurity.mask
    val ds_hashMaskRdd = filteredVideoRdd.map(m=>(m.id,m.category))
    println("**********************************************************************************************")
    println("New Usecase 8 - Masking the Category column using hashMask method")
    ds_hashMaskRdd.take(20).foreach(i=>println(i._1,objmask.hashMask(i._2)))
  }
}