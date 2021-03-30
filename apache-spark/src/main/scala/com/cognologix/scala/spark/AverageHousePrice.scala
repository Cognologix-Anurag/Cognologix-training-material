package com.cognologix.scala.spark

/**
 * Create a Spark program to read the house data from in/RealEstate.csv,
 * output the average price for houses with different number of bedrooms.
 * <p>
 * The houses dataset contains a collection of recent real estate listings in 
 * San Luis Obispo county and
 * around it. 
 * <p>
 * The dataset contains the following fields:
 * 1. MLS: Multiple listing service number for the house (unique ID).
 * 2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
 * northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
 * some out of area locations as well.
 * 3. Price: the most recent listing price of the house (in dollars).
 * 4. Bedrooms: number of bedrooms.
 * 5. Bathrooms: number of bathrooms.
 * 6. Size: size of the house in square feet.
 * 7. Price/SQ.ft: price of the house per square foot.
 * 8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.
 * <p>
 * Each field is comma separated.
 * <p>
 * Sample output:
 * <p>
 * (3, 325000)
 * (1, 266356)
 * (2, 325000)
 * ...
 * <p>
 * 3, 1 and 2 mean the number of bedrooms.
 * 325000 means the average price of houses with 3 bedrooms is 325000.
 */
object AverageHousePrice {

  /*
    Implement by using reduce operation on RDD
    Also implement this problem using Pair RDD.
   */
   def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "AverageHousePrice")

    val data = sc.textFile("in/RealEstate.csv")


    val fltr = data.filter( line => !line.startsWith("MLS"))
    val rdd = fltr.map(line => (line.split(",")(3).toInt, line.split(",")(2).toFloat.toInt))

    val totalsByRoom = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

       
    val averagesByRoom = totalsByRoom.mapValues(x => x._1 / x._2)
    val results = averagesByRoom.collect()
     results.sorted.foreach(println)
  }
}
