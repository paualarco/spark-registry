package scala.com.paualarco.scenario

import org.apache.spark.sql.SaveMode

import scala.com.paualarco.ResourcesPool.{hs, ss}

trait TestCommonParams {

  val dbName = "analytics"
  val noRegistryTableName = "no_registry"
  val registryTableName = "registry"

  def setUpEnvironment(csvPath: String): Unit = {
    hs.createDataBase(dbName)
    val df = ss
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv(csvPath)
    df.write.mode(SaveMode.Overwrite).format("orc").saveAsTable(s"$dbName.`$noRegistryTableName`")
    df.write.mode(SaveMode.Overwrite).format("orc").saveAsTable(s"$dbName.`$registryTableName`")

  }

  def cleanEnvironment {
    hs dropTable (dbName, noRegistryTableName)
    hs dropTable (dbName, registryTableName)


  }
}
