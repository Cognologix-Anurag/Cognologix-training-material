package com.cognologix.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;


public class JavaSparkTrial {

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkFileSumApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("apache-spark/in/airports.text");
        //JavaRDD<String> lines;
        input.flatMap(s-> Arrays.asList(s.split(",")).iterator()).foreach(s -> System.out.println(s));

    }
}

