package com.bigdata.spark.sparksql
import com.bigdata.spark.sparksql.sparkallfunction._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession

object sparkfunpracties {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\au-500.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    val ndf1 = df.select($"state", $"email")
    df.select("*").where($"email".like("%yahoo%"))

    ndf1.createOrReplaceTempView("tab")


    //convert to udf
    val off = udf(octberoffers _)
    spark.udf.register("Diwallioff", off )
   //val ndf = spark.sql("select *, Diwallioff(state) diwallioffers from tab")
    val ndf = ndf1.withColumn("offers", off($"state"))
    ndf.show()


    spark.stop()
  }
}
