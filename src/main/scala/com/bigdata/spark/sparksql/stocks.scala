package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object stocks {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val data  = "file:///C:\\work\\datasets\\jsondataset\\stocks.json"
    val stocks = spark.read.format("json").option("inferSchema","true").load(data)
   val res=  stocks.createOrReplaceTempView("tab")
    stocks.printSchema()
    val reg = "[^\\p{L}\\p{Nd}]+"
    //except string and integer will be removed
      val columns=stocks.columns.map(x=>x.replaceAll(reg,""))
    val df = stocks.toDF(columns:_*).withColumn("Company",regexp_replace($"Company",reg,"")).withColumnRenamed("Company", "msivaram")
   //toDF will be used 1.to convert RDD into dataframe 2.Rename all columns in oneshort
    df.printSchema()
    df.show()
    spark.stop()
  }
}
