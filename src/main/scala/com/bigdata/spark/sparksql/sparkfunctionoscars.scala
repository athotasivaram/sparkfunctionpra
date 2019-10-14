package com.bigdata.spark.sparksql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object sparkfunctionoscars {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
val data = "file:///C:\\work\\datasets\\Oscars.txt"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","\t").load(data)
    val osc = df.select("birthplace", "date_of_birth", "race_ethnicity","year_of_award","award")
      .toDF("birthplace", "date_of_birth", "race","award_year","award")
    osc.createOrReplaceTempView("awards")
    //osc.select("award").distinct().show(10,false)
    //Check DOB quality. Note that length varies based on month name
    //spark.sql("SELECT distinct(length(date_of_birth)) FROM awards ").show()
    //Look at the value with unexpected length 4.
    //spark.sql("SELECT date_of_birth FROM awards WHERE length(date_of_birth) = 4").show()
    //This is an invalid date. We can either drop this record or give some meaningful value like 01-01-1972
    //UDF to clean date
    //This function takes 2 digit year and makes it 4 digit
    // Any exception returns an empty string

    def fncleanDate(s:String) : String = {
      var cleanedDate = ""
      val dateArray: Array[String] = s.split("-")
      try{    //Adjust year
        var yr = dateArray(2).toInt
        if (yr < 100) {yr = yr + 1900 } //make it 4 digit
        cleanedDate = "%02d-%s-%04d".format(dateArray(0).toInt,
          dateArray(1),yr)
      } catch { case e: Exception => None }
      cleanedDate
    }

    //UDF to clean birthplace
    // Data explorartion showed that
    // A. Country is omitted for USA
    // B. New York City does not have State code as well
    //This function appends country as USA if
    // A. the string contains New York City  (OR)
    // B. if the last component is of length 2 (eg CA, MA)

    def fncleanBirthplace(s: String) : String = {
      var cleanedBirthplace = ""
      var strArray : Array[String] =  s.split(" ")
      if (s == "New York City")
        strArray = strArray ++ Array ("USA")
      //Append country if last element length is 2
      else if (strArray(strArray.length-1).length == 2)
        strArray = strArray ++ Array("USA")
      cleanedBirthplace = strArray.mkString(" ")
      cleanedBirthplace
    }
    //Register UDFs
    spark.udf.register("fncleanDate",fncleanDate(_:String))
    spark.udf.register("fncleanBirthplace", fncleanBirthplace(_:String))

    var cleaned_df = spark.sql (
      """SELECT fncleanDate (date_of_birth) dob,
               fncleanBirthplace(birthplace) birthplace,
               substring_index(fncleanBirthplace(birthplace),' ',-1) country,
               (award_year - substring_index(fncleanDate(date_of_birth),
                         '-',-1)) age,
               race, award FROM awards""")
    //Missing value treatment

    // Drop rows with incomplete information

    cleaned_df = cleaned_df.na.drop

    cleaned_df.groupBy("award","country").count().sort("country","award","count").show(10,false)

    cleaned_df.createOrReplaceTempView("awards")

    //Find out levels (distinct values) in each categorical variable
    spark.sql("SELECT count(distinct country) country_count, count(distinct race) race_count, count(distinct award) award_count from awards").show()

    //Distinct levels in race and award are not many.
    //Country has too many values. Retain top ones and bundle the rest
    //Check out top 6 countries with most awards.


    //Country has too many values. Retain top ones and bundle the rest
    //Check out top 6 countries with most awards.
    val top_countries_df = spark.sql("SELECT country, count(*) freq FROM awards GROUP BY country ORDER BY freq DESC LIMIT 6")
    top_countries_df.show()
    //Prepare top_countries list using map
    val top_countries = top_countries_df.select("country").collect().map(x => x(0).toString)

    //UDF to fix country. Retain top 6 and bundle the rest into "Others"
    import org.apache.spark.sql.functions.udf
    val setCountry = udf ((s: String) => { if (top_countries.contains(s)) {s} else {"Others"}})
    cleaned_df = cleaned_df.withColumn("country", setCountry(cleaned_df("country")))
    cleaned_df.show()



    df.show()
    spark.stop()
  }
}
