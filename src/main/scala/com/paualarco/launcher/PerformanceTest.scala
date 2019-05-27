package scala.com.paualarco.launcher

import org.apache.log4j.{Level, Logger}

import scala.com.paualarco.ResourcesPool.ss
import scala.com.paualarco.SparkRegistry._
import System.{currentTimeMillis => currentMillis}

class PerformanceTest(dbName: String, tableName: String) {
  val LOG = Logger.getLogger(this.getClass)
  LOG.setLevel(Level.INFO)
  LOG.info(s"DbName: $dbName")
  LOG.info(s"TableName: $tableName")

  def noRegistryTest(sqlStatement: String, iterations: Int): Long = {
    val startTime = currentMillis
    LOG.info("Performance test with NO Registry")
    LOG.info(s"$iterations iterations accessing and counting to the same hive data")
    for (i <- 1 until iterations) {
      val df = ss.sql(sqlStatement)
      val hash = df.hashCode
      val count = df.count()
      LOG.info(s"It.$i, Hash: $hash, Count: $count")
    }
    currentMillis - startTime
  }

  def registryTest(sqlStatement: String, iterations: Int): Long = {
    val startTime = currentMillis
    LOG.info("Performance test with Registry")
    LOG.info(s"$iterations iterations accessing and counting to the same hive data")
    for (i <- 1 until iterations) {
      val df = ss.sqlRegistry(sqlStatement)
      val hash = df.hashCode
      val count = df.countRegistry
      LOG.info(s"It.$i, Hash(DFRegistry): $hash, Count registry: $count")
    }
    currentMillis - startTime
  }
}