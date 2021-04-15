package com.cognologix.spark;

/*
      calculate number of words from text file
      and print it on the console.
*/

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("AverageHousePrice");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        JavaRDD<String> ip = sc.textFile("apache-spark/in/word_count.text").flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.stream(s.split(" ")).iterator();
            }
        });

        System.out.println(ip.count());

    }
}
