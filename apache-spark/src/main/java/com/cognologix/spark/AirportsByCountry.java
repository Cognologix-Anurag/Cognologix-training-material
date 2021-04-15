package com.cognologix.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

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

// Implement by using on RDD groupBy
//
// Also implement this problem using Pair RDD.
//
// Implement by using groupByKey

public class AirportsByCountry {


    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("AirportsByCountry");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        JavaRDD<String> ip = sc.textFile("apache-spark/in/airports.text");


        //using pairRDD and groupByKey

        PairFunction<String,String,String> keyValue = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2(s.split(",")[3],s.split(",")[1]);
            }
        };

        JavaPairRDD<String,String> countryAirport = ip.mapToPair(keyValue);

        //System.out.println(countryAirport.collect());

        JavaPairRDD<String,Iterable<String>> countryGroup1 = countryAirport.groupByKey();

        System.out.println(countryGroup1.collect());


        //using groupBy

        JavaPairRDD<String,Iterable<String>> countryGroup2 = ip.groupBy(x->x.split(",")[3]);

        System.out.println(countryGroup2.collect());


    }
}
