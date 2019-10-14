package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object worldbank {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql

    val data  = "file:///C:\\work\\datasets\\jsondataset\\new7.json"
    val client = spark.read.format("json").option("inferSchema","true").load(data)
    client.show()
    client.createOrReplaceTempView("tab")

    val df= spark.sql("select  _id.`$oid` OID, approvalfy,board_approval_month,boardapprovaldate, " +
      "borrower,closingdate,country_namecode,countrycode," +
      "supplementprojectflg,countryname,countryshortname," +
      " envassesmentcategorycode,grantamt,ibrdcommamt,id,idacommamt,impagency,lendinginstr, lendinginstrtype,lendprojectcost, mjthemecode,sector.name[0] name1,sector.name[1] name2, sector.name[2] name3," +
      " sector1.name S1Name, sector1.percent S1Percent," +
      "sector2.name S2Name, sector2.percent S2Percent," +
      "sector3.name S3Name, sector3.percent S3Percent," +
      "sector4.name S4Name, sector4.percent S4Percent, prodline, prodlinetext, productlinetype,"+
      " mjtheme[0] mjtheme1,mjtheme[1] mjtheme2,mjtheme[2] mjtheme3  , project_name ,projectfinancialtype, projectstatusdisplay,regionname,sectorcode, source,status,themecode,totalamt,totalcommamt,url," +
      "mp.Name mpname, mp.Percent mppercent, tn.code tncode, tn.name tnname,mn.code mncode, mn.name mnname, tc.code tccode, tc.name tcname, pd.DocDate pddocdate ,pd.DocType pddoctype, pd.DocTypeDesc pddoctypedesc, pd.DocURL  pddocurl, pd.EntityID pdid from tab lateral view explode(majorsector_percent) tmp as mp lateral view explode(theme_namecode) tmp as tn lateral view explode(mjsector_namecode) tmp as mn lateral view explode(mjtheme_namecode) tmp as tc lateral view explode(projectdocs) tmp as pd")
    df.show()
    df.printSchema()
/*val url="oracle path"
val prop = new java.util.Properties()*/

   /* val url="jdbc:oracle:thin:@//myoracledb.cdpobvgs3kxr.us-east-2.rds.amazonaws.com:1521/orcl"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")*/

    spark.stop()
  }
}
