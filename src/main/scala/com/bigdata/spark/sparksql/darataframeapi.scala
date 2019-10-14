package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object darataframeapi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql

   /*val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("cars.csv")*/
    val dat = "file:///C:\\work\\datasets\\NWOrderDetails.csv"
    val data = "file:///C:\\work\\datasets\\NWOrders.csv"
    val dat_df = spark.read.format("csv").option("header","true").option("delimiter",",").option("inferSchema","true").option("path",dat).load()
    val data_df = spark.read.format("csv").option("header","true").option("delimiter",",").option("inferSchema","true").option("path",data).load()
    //dat_df.show(4)
    //data_df.show(4)

    dat_df.createOrReplaceTempView("dat")
    data_df.createOrReplaceTempView("data")
    val datasal = spark.sql("select a.OrderID, a.UnitPrice, a.Qty, a.Discount, b.ShipCountry , (a.UnitPrice*a.Qty)-a.Discount as sale_amt from  dat a , data b, group by='ShipCountry'")
    datasal.createOrReplaceTempView("saleamt")
    val datasal1= spark.sql("select ShipCountry, sum(sale_amt)from saleamt group by ShipCountry")
    datasal1.show()



    //df.createOrReplaceTempView("tab")
   // val res = spark.sql("select * from tab where ")

   // res.show()

    //res.coalesce(1).write.format("csv").option("header","true").option("delimiter","\t").save(op)
      spark.stop()
  }
}
