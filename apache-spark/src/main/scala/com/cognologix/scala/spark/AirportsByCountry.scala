package com.cognologix.scala.spark

/**
 * Create a Spark program to read the airport data from in/airports.text,
 * output the the list of the names of the airports located in each country.
 * <p>
 * Each row of the input file contains the following columns:
 * Airport ID, Name of airport, Main city served by airport,
 * Country where airport is located, IATA/FAA code,
 * ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
 * <p>
 * Sample output:
 * <p>
 * "Canada", List("Bagotville", "Montreal", "Coronation", ...)
 * "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
 * "Papua New Guinea",  List("Goroka", "Madang", ...)
 * ...
 */
object AirportsByCountry {

  /*
    Implement by using on RDD groupBy

    Also implement this problem using Pair RDD.

    Implement by using groupByKey
   */

}
