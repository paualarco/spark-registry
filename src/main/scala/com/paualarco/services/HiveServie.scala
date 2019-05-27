package scala.com.paualarco.services

import scala.com.paualarco.ResourcesPool.ss

object HiveServie {

  def dropTable(dbName: String, tableName: String) {
    ss.sql(s"DROP TABLE IF EXISTS $dbName.`$tableName` ")
  }

  def createDataBase(dbName: String) {
    ss.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
  }

  def createTable(dbName: String, tableName: String, fieldName: String*){
    ss.sql(s"CREATE TABLE IF NOT EXISTS $dbName.$tableName (${fieldName.map(_ + " String").mkString(", ")})")
  }



}
