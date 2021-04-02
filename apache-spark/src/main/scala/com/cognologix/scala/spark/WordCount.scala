package com.cognologix.scala.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("First Spark Program").setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.textFile("apache-spark/in/word_count.text")

    /*
      calculate number of words from text file
      and print it on the console.
     */
    
    // count total number of words 
    
    val result = rdd.map(_.split(" ").length)
    print(result.sum().toInt)
    
    /* Another way to count total number of words 
    val words = rdd.flatMap(x => x.split(" "))
    println(words.count())
    */
   
    // occurrence 
    val words = rdd.flatMap(x => x.split(" "))
    val wordCount = words.countByValue()
    wordCounts.foreach(println)
   
    sparkContext.stop()
  }

}
