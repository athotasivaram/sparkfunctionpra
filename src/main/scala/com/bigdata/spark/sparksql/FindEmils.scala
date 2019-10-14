package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FindEmils {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val dat = "file:///C:\\work\\datasets\\10000 Records.csv"
    val reco = spark.read.format("csv").option("header","true").option("delimiter",",").option("inferSchema","true").option("path",dat).load()
    //reco.show()

    reco.createOrReplaceTempView("tab")
     //res.show()

    spark.stop()
  }
}
