package scala.com.paualarco.scenario

import java.lang.System.{currentTimeMillis => currentMillis}

import org.apache.log4j.{Level, Logger}

import scala.com.paualarco.ResourcesPool.ss
import scala.com.paualarco.SparkRegistry._

class PerformanceTest {
  val LOG = Logger.getLogger(this.getClass)
  LOG.setLevel(Level.INFO)


  def noRegistryTest(dbName: String, tableName: String, sqlStatement: String, iterations: Int): Long = {
    val startTime = currentMillis
    LOG.info(s"DbName: $dbName")
    LOG.info(s"TableName: $tableName")
    LOG.info("Performance scenario with NO Registry")
    LOG.info(s"$iterations iterations accessing and counting to the same hive data")
    for (i <- 1 until iterations) {
      val df = ss.sql(sqlStatement)
      val hash = df.hashCode
      val count = df.count()
      LOG.info(s"It.$i, Hash: $hash, Count: $count")
    }
    currentMillis - startTime
  }

  def registryTest(dbName: String, tableName: String, sqlStatement: String, iterations: Int): Long = {
    val startTime = currentMillis
    LOG.info(s"DbName: $dbName")
    LOG.info(s"TableName: $tableName")
    LOG.info("Performance scenario with Registry")
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