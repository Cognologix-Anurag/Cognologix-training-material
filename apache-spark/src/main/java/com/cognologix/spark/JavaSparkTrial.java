package com.cognologix.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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

public class JavaSparkTrial {

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkFileSumApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("apache-spark/in/airports.text");
        //JavaRDD<String> lines;
        input.flatMap(s-> Arrays.asList(s.split(",")).iterator()).foreach(s -> System.out.println(s));

    }
}

