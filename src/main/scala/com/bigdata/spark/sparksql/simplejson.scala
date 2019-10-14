package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object simplejson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
val data  = "file:///C:\\work\\datasets\\jsondataset\\zip.json"
    val client = spark.read.format("json").option("inferSchema","true").load(data)
    //client.show()
    client.createOrReplaceTempView("tab")
    val res = spark.sql("select _id id , city, loc[0] lang, loc[1] lati,pop,state from tab")
    res.createOrReplaceTempView("test")
    val result = spark.sql("select city, count(*) cnt from test group by city order by cnt desc")
    result.show()

    result.printSchema()

    spark.stop()
  }
}
