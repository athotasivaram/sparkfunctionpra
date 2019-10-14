package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object getMysqlOracledata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql

    //load data from oracledb
    val url ="jdbc:oracle:thin:@//myoracledb.cdpobvgs3kxr.us-east-2.rds.amazonaws.com:1521/orcl"
    val oprop=new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df =spark.read.jdbc(url, "emp" ,oprop)
     df.show()
//load data from  mysqldb

    val murl ="jdbc:mysql://mysqldb.clbm1tfz6ih0.us-east-2.rds.amazonaws.com:3306/mysqldb"
    val oprop1=new java.util.Properties()
    oprop1.setProperty("user","musername")
    oprop1.setProperty("password","mpassword")
    oprop1.setProperty("driver","com.mysql.cj.jdbc.Driver")
    val df1 =spark.read.jdbc(murl, "dept" ,oprop1)
    df1.show()
    df.createOrReplaceTempView("emp")
    df1.createOrReplaceTempView("dept")
    //join the oracle and mysql data
    val res = spark.sql("select e.ename, e.empno, e.sal,d.loc from emp e join dept d on d.deptno=e.deptno")
    res.show()
    //Export data to mssql

    val msurl ="jdbc:sqlserver://msdb.clbm1tfz6ih0.us-east-2.rds.amazonaws.com:1433;databaseName=sivaramdb"
    val oprop2=new java.util.Properties()
    oprop2.setProperty("user","msusername")
    oprop2.setProperty("password","mspassword")
    oprop2.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    res.write.jdbc(msurl,"ormyms",oprop2)
    spark.stop()
  }
}
