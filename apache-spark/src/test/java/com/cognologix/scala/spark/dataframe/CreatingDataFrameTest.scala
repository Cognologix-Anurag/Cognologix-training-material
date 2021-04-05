package com.cognologix.scala.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/*
  Refer below link for guidance
  https://spark.apache.org/docs/2.3.0/sql-programming-guide.html
 */
class CreatingDataFrameTest extends AnyFunSuite {

  var sparkSession : SparkSession = _



  test("creating dataframe from json file and display"){
    /*
        read people.json file using SparkSession
     */
  }

  test("creating dataframe from RDD and display"){
    /*
        read people.txt file using SparkContext.
        This will give you RDD
        Convert RDD to dataframe.
     */
  }



}
