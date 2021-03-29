package com.cognologix.scala.spark

/**
 * Create a Spark program to read the airport data from in/airports.text,
 * find all the airports which are located in United States
 * and output the airport's name and the city's name to out/airports_in_usa.text.
 * <p>
 * Each row of the input file contains the following columns:
 * Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
 * ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
 * <p>
 * Sample output:
 * "Putnam County Airport", "Greencastle"
 * "Dowagiac Municipal Airport", "Dowagiac"
 * ...
 */
object AirportsInUSA {
    def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "AirportsInUSA")
    val data = sc.textFile("in/airports.text")
    var result = data.filter(_.split(",")(3).contains("United States"))
    var output = result.map(line => new Tuple2(line.split(",")(1),line.split(",")(2)))

    output.saveAsTextFile("out/airports_in_usa.txt")
    sc.stop()
  }

}
