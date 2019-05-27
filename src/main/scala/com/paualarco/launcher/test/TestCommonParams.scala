package scala.com.paualarco.launcher.test

import org.apache.spark.sql.SaveMode

import scala.com.paualarco.ResourcesPool.{hs, ss}

trait TestCommonParams {

  val dbName = "analytics"
  val tableName = "stackOverflow"
  def setUpEnvironment(csvPath: String): Unit = {
    hs.createDataBase(dbName)
    val df = ss
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv(csvPath)
    df.write.mode(SaveMode.Overwrite).format("orc").saveAsTable(s"$dbName.`$tableName`")
  }

  def cleanEnvironment {
    hs dropTable (dbName, tableName)
  }
}
