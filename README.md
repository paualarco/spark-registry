#Spark-Registry 

##Table of contents 
- [ Introduction ] 
- [ State of the Art ]
- [ Enhancements ]
    - [ Spark Session Extension ]
    - [ Spark DataFrame Extension ]
- [ Performance tests ]
    - [ Local ScalaTest ]
    - [ AWS Cluster ]
    
    
###Introduction

The use of the Spark Registry, improves the performance on queries and count actions for those iterative Spark Applications that runs 
onto an static hive data (data taken as input from hive that we know it is not going to change during the execution time of application).
It is achieved by creating an extension to the SparkSession and DataFrame API of Spark 2.4.0.
In order to evaluate the improvement, the registry was tested with two mocked application, one that used it and another that not.
That tests were executed in local using ScalaTest and also on the Cloud using AWS.

###State of the Art
The SparkRegistry object contains the logic of the two extensions of the current SparkSession and DataFrame apis. 
An 'extension' is a new method that is appended to an existing api. This is a handy and clean procedure achieved by 
using scala implicits. Therefore, two implicit statements were defined in our SparkRegistry class:
```scala
implicit def exendedSparkSession(ss: SparkSession): SparkSessionExtension = new SparkSessionExtension(ss)
implicit def extendedDataFrame(df: DataFrame): DataFrameExtension = new DataFrameExtension(df)
  ```
The implicit definition takes as parameter an SparkSession or the DataFrame apis, returning as a result a custom 
class respectively called SparkSessionExtension and DataFrameExtension, in which we could use their methods as if they were 
part of the original API.
In order to perform the implicit transformations (SparkSession -> SparkSessionExtended, DataFrame -> DataFrameExtension) we just have to 
import the package methods in the class where we would like to have this functionality:
```scala
import scala.com.paualarco.SparkRegistry._
```

###Enhancements

The objective of that Registry class was to substantially improve the elapsed times on reading and performing count
actions on any Spark Application.
Let´s explain the two different enhancements:

####Spark Session Extension

The aim of the SparkSession extension is to reduce the elapsed time for those batch applications that 
need to access multiple times to the same hive dataset over the application lifecycle. If you are thinking, why don´t 
acess to the data once, storing the results in a dataframe variable and then access this dataframe as much times as you want?´
This could be a solution, but that would not work for those applications that have implemented a wide range of 
different methods that are  supposed to receive a reference to the database and table as parameters instead of the dataframe. 
So, this is an example of a data certification api, that could have different methods that applied different
rules in order to check whether the data is correct or not, an example of a certification method is:
 
 ```scala
def validationQuery1(dataBase: String, tableName: String, date: String): Boolean ={
  val validationDF = hiveCtx.sql(s"SELECT * FROM $dataBase.`$tableName` WHERE date=$date")
  (validationDF.count > 0) //validation Condition
}
 ```
Now, imagine that we have an application with tons of validation methods, then, the selected data has to
 be retrieved from hive every time a validation query is triggered, even whe the it is using exactly the same query.
In order to improve that, you can import the `exendedSparkSession` method from SparkRegistry class.
What this method does is to convert the SparkSession into a custom class called SparkSessionExtended:
```scala
val queryRegistry: MutableMap[String, DataFrame] = MutableMap()
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
```
The method `sqlRegistry` is appended to the SparkSession: 
```scala
import scala.com.paualarco.SparkRegistry._
val ss = SparkSession.builder()...getOrCreate()
ss.sqlRegistry("SELECT * FROM db1.table1")
```
Now, every time we call the sqlRegistry method with the same SQL statement, it will directly access the already saved DataFrame from the
queryRegistry. 
The query registry is a mutable HashMap that uses the SqlStatement as a key, and the result DataFrame as value to that key.
As a result, the query to hive is only performed once, the first time. But it could cause unalignment if the hive data i updated, since we are going to access
the first snapshot that was taken.  

####Data Frame Extension
A new method was appended to the Data Frame api using the DataFrameExtension class. This was again achieved with implicit definitions.
Te method countRegistry, is saving the count of the given DataFrame in countsRegistry HashMap, in that case 
the dataframe hash is used as identifier or key of the given Map. What we achieve doing that is to reduce the time on performing count 
of the same dataframe by only performing that operation once.
```scala
val countsRegistry: MutableMap[Int, Long] = MutableMap()
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
```

###Performance tests
Two processes with the same behaviour, one with registry and the other without were ran
in order to demonstrate that the one that uses SparkRegistry is getting better total elapsed time.
Both proocesses are written in the same class com.paualrco.scenario.PerformanceTest as noRegistryTest 
and registryTes taking as parameter the sql statement and the number of iterations that the process will
do. Each iteration will be composed of a query to get data from hive, and a count over the result dataframe.

####Local test
The local test is performed using the scala test plugin and winutils to emulate a hive data warehouse in our machine.
You can find this test in the test folder test.scala.com.paualarco.SparkRegistryTest in which basically 
it is testing that the process that runs with registry is faster than the one that runs without.
```scala
 "Spark with registry" should "be faster than without" in {
    val noRegistry = noRegistryTest(dbName, noRegistryTableName, s"SELECT * FROM $dbName.`$noRegistryTableName`", 20)
    val registry = registryTest(dbName, registryTableName, s"SELECT * FROM $dbName.`$registryTableName`", 20)
    println(s"No registry elapsed time: $noRegistry")
    println(s"Registry elapsed time: $registry")
    assert(noRegistry>registry)
  }
```
The test was realized with a process of 20 iterations using a simple sqlQuery to a csv file of 70MB of size
taken from stackOverflow.  
Here you have the results of that test:

![LocalScalaTest]:[localScalaTest]
 
  //hdfs dfs -rm -r /user/spark/warehouse/analytics.db/stackoverflow
  //spark-submit --class scala.com.paualarco.launcher.RegistryLauncher spark-registry-1.0-SNAPSHOT.jar yarn-client s3n://paualarco-spark-on-aws/survey_results_public.csv 6
//spark-submit --class scala.com.paualarco.launcher.NoRegisrtyLauncher spark-registry-1.0-SNAPSHOT.jar yarn-client s3n://paualarco-spark-on-aws/survey_results_public.csv 6

[localScalaTest]: img/Local%20Results.PNG