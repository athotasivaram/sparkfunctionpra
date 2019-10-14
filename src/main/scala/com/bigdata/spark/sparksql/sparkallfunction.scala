package com.bigdata.spark.sparksql
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkallfunction {
  def octoff(state: String) = state match {
    case "OH" => "30% off"
    case "NJ" => "20% off"
    case ("CA" | "TX") => "50% off"
    case _ => "9% off"
  }

    def octberoffers(state: String) = state match {
      case "WA" => "90% offer"
      case "NSW" => "70% offer"
      case ("QLD" | "TAS") => "50% offer"
      case _ => "30% offer"
    }
  def octboffer(balance: Int) = balance match {
    case 10000 | 15000 => "20% offer"
    case 20000 => "30% offer"
    case 30000 => "70% offer"
    case 1350 => "15% offer"
    case 100 => "10% offer"
    case _ => "5% offer"
  }


}
