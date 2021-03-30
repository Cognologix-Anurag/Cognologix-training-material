package com.cognologix.scala.spark

/**
 * "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
 * "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
 * Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
 * take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.
 * <p>
 * Keep in mind, that the original log files contains the following header lines.
 * host	logname	time	method	url	response	bytes
 * <p>
 * Make sure the head lines are removed in the resulting RDD.
 */
object UnionLogProblem {
   def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "HelloWorld")

    val july_log = sc.textFile("in/nasa_19950701.tsv")
    val august_log = sc.textFile("in/nasa_19950801.tsv")
    val result = july_log.union(august_log)
    val result1 = result.filter( line => !(line.startsWith("host") && line.contains("bytes")))
    val result2 = result1.sample(withReplacement = true, fraction = 0.1)

    result2.saveAsTextFile("out/sample_nasa_logs.csv")
    sc.stop()
  }

}
