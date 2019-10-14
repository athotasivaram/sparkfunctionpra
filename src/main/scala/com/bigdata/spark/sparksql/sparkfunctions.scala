package com.bigdata.spark.sparksql
import com.bigdata.spark.sparksql.sparkallfunction._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession

object sparkfunctions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql

    val data = "file:///C:\\work\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").load(data)
    //concatinate two or more columns
   // val ndf = df.select($"first_name", $"last_name",concat_ws(" ",$"first_name", $"last_name").alias("fullname"))
    //add extra column based on condition
    //current_date return today date
    //to add 1,2,3,4.. unique numbers use it monotonically_increasing_id
   // val ndf1 = df.withColumn("id", monotonically_increasing_id()-1)
    // lit just add dummy values
   // val ndf2 = df.withColumn("test",lit("0"))
    //regex_replace used to replace something in the column based on pattern
    //scala dsl function //domain specific lang
  /* val ndf2 = df.withColumn("phone1", regexp_replace($"phone1","-",""))
     .withColumn("fullname",concat_ws("-",$"first_name", $"last_name"))
     .withColumn("state",when($"state"==="OH" ,"OHIO").when($"state"==="NJ","NewJ").otherwise($"state"))
     .drop("first_name","last_name")*/
    //spark sql approach
    df.createOrReplaceTempView("tab")
    //val res = spark.sql("select *,when(state=='OH','Ohio').otherwise(state) newcol from tab")

    //convert to udf
    val off = udf(octoff _)
    spark.udf.register("todayoffer", off)
    //val ndf = df.withColumn("offer", off($"state"))
    val ndf = spark.sql("select *, todayoffer(state) todayoffers  from tab")
    ndf.show()

    spark.stop()
  }
}
