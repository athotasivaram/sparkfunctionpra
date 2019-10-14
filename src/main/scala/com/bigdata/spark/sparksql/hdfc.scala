package com.bigdata.spark.sparksql
import com.bigdata.spark.sparksql.sparkallfunction._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession

object hdfc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
val data = "file:///C:\\work\\datasets\\bank.csv"
    val df = spark.read.format("csv").option("header","true").option("delimiter",";").option("inferSchema","true").load(data)
    val ndf = df.select($"balance",$"loan",$"job")

    //ndf1.show()
     ndf.createOrReplaceTempView("tab")
    //convet to udf
    val off = udf(octboffer _)
    spark.udf.register("offers", off)
   val ndf1 = ndf.withColumn("goodbalance", off($"balance"))
    ndf1.show()

    //val res = spark.sql("select * from tab where balance>20000").show()

    spark.stop()
  }
}
