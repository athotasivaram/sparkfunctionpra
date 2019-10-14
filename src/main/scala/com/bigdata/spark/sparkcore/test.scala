package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object test {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql

    val names = Array("kumar","siva","ram","roger")
    val nrdd = sc.parallelize(names)
     val process = nrdd.map(x=>x.toUpperCase)
    process.collect.foreach(println)

    val data = "file:///C:\\work\\datasets\\au-500.csv-"
    spark.stop()
  }
}
