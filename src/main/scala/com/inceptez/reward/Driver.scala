package com.inceptez.reward

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

import java.io._
import java.sql.{Connection, Statement,DriverManager}


object Driver {
  
  def main(args:Array[String])=
  {
    //Read from Kafka -> Lookup with hbase -> calculate reward points -> Mysql
    
    
    try
    {
      val spark = SparkSession.builder().appName("RewardProcess").master("local[*]").getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      
      //Step 1: Read from kafka
      val df = spark.readStream
     .format("kafka")
     .option("kafka.bootstrap.servers", "localhost:9092")
     .option("subscribe", "paymenttopic")
     .load().selectExpr("CAST(timestamp AS TIMESTAMP)", "CAST(value AS STRING)")
    
     val df1 =  df.select(split(df("value"), ",").alias("credit"))
    
     //transid,custid,card,amount,merchant,event_dt
     //072109,12,1111,500,Restaurant,2022-03-14 07:21:09
     val df2 = df1.select(col("credit").getItem(0).alias("transid").cast(LongType),
                         col("credit").getItem(1).alias("customer_id").cast(IntegerType),
                         col("credit").getItem(2).alias("card_no"),
                         col("credit").getItem(3).alias("amount").cast(FloatType),
                         col("credit").getItem(4).alias("merchant_name"),
                         col("credit").getItem(5).alias("event_dt"))
    
    val df3 = df2.withColumn("process_ts", current_timestamp())
    
    //df3.writeStream.format("console").option("truncate",false).start().awaitTermination()
    
    //Step 2 : lookup with hbase and write into mysql
    df3.writeStream
       .foreachBatch(processandsave _)
       .outputMode("append")
       .start()
       .awaitTermination()
    
        
    }
    catch
    {
      case ex: Exception => println("Other Exception")
      
    }
    
  }
  
  def processandsave (df: DataFrame, batchId: Long) = 
  {
    
    //extracting customer id from the dataframe 
    val customerlist = df.select("customer_id").collect().map(col => col.getInt(0).toString())
    
    if(customerlist != null && customerlist.length > 0)
    {
      
      //read data from hbase table for the customerlist
      val customerid_list = customerlist.mkString(",")
      println(customerid_list)
      
      val rinfo = enrichdata(customerid_list)
      
      val df1 = df.withColumn("rinfo", addrewardinfo(rinfo)(col("customer_id"),col("amount")))
      //rinfo= Array("No","Air Miles","Low","50")
      
      val df2 = df1.selectExpr("transid","customer_id","event_dt","amount","card_no","merchant_name","process_ts","rinfo[0] as blocked",
          "rinfo[1] reward_type","rinfo[2] as credit_rating","cast(rinfo[3] as int) as reward_point")
    
      //Write the enriched data into mysql table    
      df2.write.format("jdbc")
         .option("url", "jdbc:mysql://localhost/reward_db")
         .option("dbtable", "tbl_customerrewards")
         .option("user", "root")
         .option("password", "Root123$")
         .mode("append")
         .save()
         
    println("written into mysql")
      df2.printSchema()
     df2.write.format("org.apache.phoenix.spark")
      .mode("overwrite")
      .option("table", "tbl_rewardcustomertrans")
      .option("zkUrl", "localhost:2181")
      .save()
      
      println("written into hbase")
    }
      
    
    
  }
  //read data from hbase table  
  def enrichdata(customeridlist:String)=
  {
    val rinfo = scala.collection.mutable.Map[Int,Array[String]]()
    val connection = DriverManager.getConnection("jdbc:phoenix:localhost");
    val statement = connection.createStatement();
    val ps = connection.prepareStatement("select customer_id,blocked,reward,credit_rating from customer_rewardinfo where customer_id in ("+ customeridlist  +")");
    val rs = ps.executeQuery();
     while(rs.next()) 
     {
       rinfo(rs.getInt(1)) = Array(rs.getString(2),rs.getString(3),rs.getString(4))
       //Map(25 -> Array('No','Air Miles','Low'))
     }
     
     statement.close()
     connection.close()
     rinfo
  }
  
  
  def addrewardinfo(rewardinfo:scala.collection.mutable.Map[Int,Array[String]])=
  {
     udf((custid:Int,amount:Float) => 
     {
       val rinfo = rewardinfo.get(custid).get
       println(rinfo.toList)
       val point = rinfo(1).toLowerCase() match 
       {
        case "air miles" => 
          if(rinfo(2).toLowerCase() == "low") 
            (amount / 100) * 1
          else if(rinfo(2).toLowerCase() == "medium") 
            (amount / 100) * 2
          else
            (amount / 100) * 3
            
        case "cash back" => 
          if(rinfo(2).toLowerCase() == "low") 
            (amount / 100) * 2
          else if(rinfo(2).toLowerCase() == "medium") 
            (amount / 100) * 4
          else
            (amount / 100) * 6
          
          
        case "points" => 
          if(rinfo(2).toLowerCase() == "low") 
            (amount / 100) * 5
          else if(rinfo(2).toLowerCase() == "medium") 
            (amount / 100) * 10
          else
            (amount / 100) * 16
          
        case _ => 0.0
      }
       rinfo :+ point.toString()
       //Array('No','Air Miles','Low',50)
     })
  }  
  
  
   
    
  
}