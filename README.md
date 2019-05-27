#Spark-Registry 

###Introduction

The aim of that Spark-Registry was to improve the performance on queries and count actions for those iterative Spark Applications that runs 
onto an static hive data (data taken as input from hive that we know it is not going to change during the execution time of application).
Spark Registry is an extension to the SparkSession and DataFrame api of Spark 2.4.0.

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

####SparkSessionExtension

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
The method `sqlRegistry` is appended to the SparkSession, so we can perform 
```scala
import scala.com.paualarco.SparkRegistry._
val ss = SparkSession.builder()...getOrCreate()
ss.sqlRegistry("SELECT * FROM db1.table1")
```
Now, every time we call the sqlRegistry method with the same SQL statement, it will directly take the already saved DataFrame to the method
is called

  //hdfs dfs -rm -r /user/spark/warehouse/analytics.db/stackoverflow
  //spark-submit --class scala.com.paualarco.launcher.RegistryLauncher spark-registry-1.0-SNAPSHOT.jar yarn-client s3n://paualarco-spark-on-aws/survey_results_public.csv 6
//spark-submit --class scala.com.paualarco.launcher.NoRegisrtyLauncher spark-registry-1.0-SNAPSHOT.jar yarn-client s3n://paualarco-spark-on-aws/survey_results_public.csv 6
