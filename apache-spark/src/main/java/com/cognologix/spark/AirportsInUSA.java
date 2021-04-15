package com.cognologix.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

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


public class AirportsInUSA {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("AirportsInUSA");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

        JavaRDD<String> ip = sc.textFile("apache-spark/in/airports.text");

        PairFunction<String,String,Tuple2<String,String>> keyValue = new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                return new Tuple2(s.split(COMMA_DELIMITER)[3],new Tuple2<>(s.split(COMMA_DELIMITER)[1],s.split(COMMA_DELIMITER)[2]));
            }
        };
        JavaPairRDD<String, Tuple2<String,String>> airportCityCountry = ip.mapToPair(keyValue);

        JavaPairRDD<String,Tuple2<String,String>> airportCity = airportCityCountry.filter(new Function<Tuple2<String, Tuple2<String, String>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                return stringTuple2Tuple2._1.equalsIgnoreCase("United States");
            }
        });

        System.out.println(airportCity.collect());
        airportCity.saveAsTextFile("apache-spark/out/airports_in_USA.text");

    }

}
