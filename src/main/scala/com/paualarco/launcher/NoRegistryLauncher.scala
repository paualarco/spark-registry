package scala.com.paualarco.launcher

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.com.paualarco.ResourcesPool.{hs, ss}
import scala.com.paualarco.scenario.{PerformanceTest, TestCommonParams}

object NoRegistryLauncher extends TestCommonParams{
  val LOG = Logger.getLogger(this.getClass)
  LOG.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val Array(deployMode, s3FilePath, iterations) = args
    LOG.info(s"DeployMode: $deployMode, S3FilePath: $s3FilePath, Iterations: $iterations")

    ss = SparkSession.builder().appName("Spark-No-Registry-Test").getOrCreate()

    setUpEnvironment(s3FilePath)

    val sqlStatement = s"SELECT * FROM $dbName.`$noRegistryTableName`"
    val noRegistry = (new PerformanceTest()).noRegistryTest(dbName, noRegistryTableName, sqlStatement, iterations.toInt)
    LOG.info(s"No registry scenario elapsed time: $noRegistry")
  }

}