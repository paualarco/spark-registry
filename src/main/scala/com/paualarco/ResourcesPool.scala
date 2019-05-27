package scala.com.paualarco

import org.apache.spark.sql.SparkSession

import scala.com.paualarco.services.HiveServie

object ResourcesPool {
  var ss: SparkSession = _
  val hs = HiveServie
}
