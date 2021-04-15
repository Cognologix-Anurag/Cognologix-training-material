package com.cognologix.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

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

public class AirportsByLatitude {

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("AirportsByLatitude");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

        JavaRDD<String> ip = sc.textFile("apache-spark/in/airports.text");


        JavaRDD filtered =ip.map(line->line.split(COMMA_DELIMITER)).map(new Function<String[], Tuple2<String,Double>>() {
            @Override
            public Tuple2<String,Double> call(String[] strings) throws Exception {
                return new Tuple2(strings[1],Double.parseDouble(strings[6]));
            }
        }).filter(k->((Tuple2<String,Double>)k)._2>40);
        System.out.println(filtered.collect());


        filtered.saveAsTextFile("apache-spark/out/airports_by_latitude.text");


    }
}
