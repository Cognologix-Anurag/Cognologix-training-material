package com.cognologix.scala.spark

/**
 * Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
 * Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.
 * <p>
 * Each row of the input file contains the following columns:
 * Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
 * ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
 * <p>
 * Sample output:
 * "St Anthony", 51.391944
 * "Tofino", 49.082222
 * ...
 */
object AirportsByLongitude {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "AirportsByLongitude")
    val data = sc.textFile("in/airports.text")
    
    var result = data.filter(_.split(",")(7).toDouble > 40.0000000)
    var output = result.map(line => new Tuple2(line.split(",")(1),line.split(",")(7)))
    output.saveAsTextFile("out/airports_by_Longitude.txt")

    sc.stop()
  }

}
