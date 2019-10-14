package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object findrddEmails {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql

val data = "file:///C:\\work\\datasets\\us-500.csv"
   val op = "file:///C:\\work\\datasets\\m"
    val brdd = sc.textFile(data)
    val mes = brdd.flatMap(x=>x.split(" ")).filter(x=>x.contains("@yahoo.com"))
    mes.take(10).foreach(println)
    mes.coalesce(1).saveAsTextFile(op)

    spark.stop()
  }
}
