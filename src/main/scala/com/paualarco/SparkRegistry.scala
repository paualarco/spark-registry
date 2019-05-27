package scala.com.paualarco

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.{HashMap => MutableMap}

object SparkRegistry {

  val LOG = Logger.getLogger(this.getClass)
  LOG.setLevel(Level.INFO)

  implicit def exendedSparkSession(ss: SparkSession): SparkSessionExtension = new SparkSessionExtension(ss)
  implicit def extendedDataFrame(df: DataFrame): DataFrameExtension = new DataFrameExtension(df)

  val queryRegistry: MutableMap[String, DataFrame] = MutableMap()
  val countsRegistry: MutableMap[Int, Long] = MutableMap()

  class SparkSessionExtension(ss: SparkSession) {
    def sqlRegistry(sqlStatement: String): DataFrame = {
      if (queryRegistry.contains(sqlStatement)) {
        LOG.info(s"Recoveriyng already registered query: $sqlStatement")
        queryRegistry(sqlStatement)
      }
      else {
        val newQueryDF = ss.sql(sqlStatement)
        LOG.info(s"New query added to the collection $sqlStatement")
        queryRegistry(sqlStatement) = newQueryDF
        newQueryDF
      }
    }
  }

  class DataFrameExtension(df: DataFrame){
    def countRegistry: Long = {
      val hash = df.hashCode
      if(countsRegistry.contains(hash)){
        LOG.info(s"The count operation was already performed on DF with hash: $hash")
        countsRegistry(hash)
      }
      else {
        LOG.info(s"New count operation on DF with hash: $hash")
        countsRegistry(hash) = df.count()
        countsRegistry(hash)
      }
    }
  }

}
