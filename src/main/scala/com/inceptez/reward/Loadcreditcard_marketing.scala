package com.inceptez.reward
import org.apache.spark.sql.SparkSession

object Loadcreditcard_marketing {
  
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("LoadCreditcard-HBase").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     
    val df = spark.read.format("csv")
                  .option("header", true)
                  .option("inferschema", true)
                  .load("file:/home/hduser/sparkdata/creditcardrewardinfo.csv")
    
    df.write.format("org.apache.phoenix.spark")
      .mode("overwrite")
      .option("table", "customer_rewardinfo")
      .option("zkUrl", "localhost:2181")
      .save()
      
    println("Data loaded into HBase through phoenix")
    
    //df.show()
  }
  
}