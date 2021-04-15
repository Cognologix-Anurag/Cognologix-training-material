package com.cognologix.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;

/**
 * "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
 * "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
 * Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
 * Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.
 * <p>
 * Example output:
 * vagrant.vf.mmc.com
 * www-a1.proxy.aol.com
 * .....
 * <p>
 * Keep in mind, that the original log files contains the following header lines.
 * host	logname	time	method	url	response	bytes
 * <p>
 * Make sure the head lines are removed in the resulting RDD.
 */


public class SameHostsProblem {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("AverageHousePrice");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        Function2<Integer, Iterator<String>, Iterator<String>> noHeader = new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                if(integer==0 && stringIterator.hasNext()){
                    stringIterator.next();
                    return stringIterator;
                }else
                    return stringIterator;
            }
        };

        JavaRDD<String> ipJuly = sc.textFile("apache-spark/in/nasa_19950701.tsv").mapPartitionsWithIndex(noHeader,false).
                map(x->x.split("\t")[0]);
        JavaRDD<String> ipAug = sc.textFile("apache-spark/in/nasa_19950801.tsv").mapPartitionsWithIndex(noHeader,false).
                map(x->x.split("\t")[0]);

        //System.out.println(ipAug.collect());

        /***intersection of host RDDs***/

        JavaRDD<String> sameHosts = ipJuly.intersection(ipAug);

        //System.out.println(sameHosts.collect());

        sameHosts.saveAsTextFile("apache-spark/out/nasa_logs_same_hosts.csv");


    }
}
