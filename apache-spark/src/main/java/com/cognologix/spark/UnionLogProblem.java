package com.cognologix.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;
import java.util.List;

/**
 * "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
 * "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
 * Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
 * take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.
 * <p>
 * Keep in mind, that the original log files contains the following header lines.
 * host	logname	time	method	url	response	bytes
 * <p>
 * Make sure the head lines are removed in the resulting RDD.
 */

public class UnionLogProblem {

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

        JavaRDD<String> ipJuly = sc.textFile("apache-spark/in/nasa_19950701.tsv").mapPartitionsWithIndex(noHeader,false);
        JavaRDD<String> ipAug = sc.textFile("apache-spark/in/nasa_19950801.tsv").mapPartitionsWithIndex(noHeader,false);


        JavaRDD<String> unionLogs = ipJuly.union(ipAug);
        //System.out.println(ipJuly.count());
        //System.out.println(ipAug.count());
        //System.out.println(unionLogs.count());

        JavaRDD<String> sampleLogs= sc.parallelize(unionLogs.takeSample(false,2000));
        sampleLogs.saveAsTextFile("apache-spark/out/sample_nasa_logs.tsv");
    }
}
