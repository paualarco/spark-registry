package scala.com.paualarco

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import scala.com.paualarco.ResourcesPool.ss
import scala.com.paualarco.launcher.{PerformanceTest, TestCommonParams}

class SparkRegistryTest extends FlatSpec with BeforeAndAfter with Matchers with TestCommonParams  {

  before {
    ss = SparkSession.builder().appName("Spark-Registry-Test").master("local").getOrCreate()
    setUpEnvironment("src/test/resources/survey_results_public.csv")
  }

  "Spark with registry" should "be faster than without" in {
    val test = new PerformanceTest(dbName, tableName)
    val iterations = 20
    val sqlStatement = s"SELECT * FROM $dbName.`$tableName`"
    val noRegistry = test.noRegistryTest(sqlStatement, iterations)
    val registry = test.registryTest(sqlStatement, iterations)
    println(s"No registry elapsed time: $noRegistry")
    println(s"Registry elapsed time: $registry")
    assert(noRegistry>registry)
  }

  after {
    cleanEnvironment
  }
}
