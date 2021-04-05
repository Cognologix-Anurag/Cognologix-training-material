package com.cognologix.scala.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}

/**
 *https://mrpowers.medium.com/manually-creating-spark-dataframes-b14dae906393
  *<br/>
  *Complete Test cases from test class
    *<link>com.cognologix.scala.spark.dataframe.CreatingDataFrameWithCaseClassTest</link>
 */
object DataFrameAndSchemaClass {

  def main(args: Array[String]): Unit = {
    var spark : SparkSession = ???

    /*
      create dataframe from Sequence and Convert it to schema
     */
    val someData = Seq(
      Row(8, "bat"),
      Row(64, "mouse"),
      Row(-27, "horse")
    )


    /*
      Create a schema for the above list using StructType
     */

    //Use Struct type

    /*val someDF = spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(someSchema)
    )*/



    /*
      create dataframe using case class
     */


  }


}
