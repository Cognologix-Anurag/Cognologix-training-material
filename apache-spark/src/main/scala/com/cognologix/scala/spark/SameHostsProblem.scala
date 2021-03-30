package com.cognologix.scala.spark

/**
 * "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
 * "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
 * Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
 * Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.
 * <p>
 * Example output:
 * vagrant.vf.mmc.com
 * www-a1.proxy.aol.com
 * .....
 * <p>
 * Keep in mind, that the original log files contains the following header lines.
 * host	logname	time	method	url	response	bytes
 * <p>
 * Make sure the head lines are removed in the resulting RDD.
 */
object SameHostsProblem {
  
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "HelloWorld")

    val july_log = sc.textFile("in/nasa_19950701.tsv")
    val august_log = sc.textFile("in/nasa_19950801.tsv")
    
    val july_host = july_log.map(_.split("\t")(0))
    val august_host = august_log.map(_.split("\t")(0))
    val result = july_host.intersection(august_host)
    val result1 = result.filter(_ != "host")
    
    result1.saveAsTextFile("out/nasa_logs_same_hosts.csv")
    sc.stop()
  }

}
