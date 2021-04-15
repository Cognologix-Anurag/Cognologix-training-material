package com.cognologix.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Write Spark program to read input file  - 2015-12-12.csv.gz
 * Analyze the files and write code to answer following questions
 */

  /*
    Analyze the dataset :- Print the first 10 rows.
   */

  /*
    Analyze the dataset using takeSample() method as well.
   */

  /*
    Convert the rows to array
   */

  /*
    Reduce and Counting
    how many downloading records each package has
   */

  /*
  Print the rankings of these packages based on how many downloads they have.
   */

  /*
    obtain these downloading records of R package "Rtts" from Germany (DE)
   */

/*
  give a mapping table of the abbreviates of four countries and their full name.
  ('DE', 'Germany'), ('US', 'United States'), ('CN', 'China'), ('IN',"India")
  print the result with countries full name
 */
public class RLogsDataAnalysis {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("AverageHousePrice");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        JavaRDD<String> ip = sc.textFile("apache-spark/in/2015-12-12.csv.gz");

        System.out.println(ip.collect().get(0));
        System.out.println(ip.collect().get(1));

        Function2 removeHeader = new Function2<Integer, Iterator<String>,Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                if(index==0 && iterator.hasNext()){
                    iterator.next();
                    return iterator;
                }else
                    return  iterator;
            }
        };

        JavaRDD<String> inputNoHeader = ip.mapPartitionsWithIndex(removeHeader,false);

        /***First 10 records***/
        //System.out.println(inputNoHeader.take(10));

        /***takeSample***/
        //System.out.println(inputNoHeader.takeSample(true,10));

        /***convert to array***/
        String[] collect = inputNoHeader.collect().toArray(new String[0]);


        /***counting number of records for each package***/

        JavaPairRDD<String,Iterable<String>> groupByPackage = inputNoHeader.groupBy(p->p.split(",")[6]);

        JavaPairRDD<String,Long> countByPackage= groupByPackage.mapValues(records -> {
            return records.spliterator().getExactSizeIfKnown();
        });

        //System.out.println(countByPackage.collect());

        /*****ranking packages by number of downloads****/

        JavaPairRDD<Integer, String> sortingRDD = countByPackage.mapToPair(x->new Tuple2(x._2.intValue(),x._1));
        sortingRDD=sortingRDD.sortByKey();
        //System.out.println(sortingRDD.collect());

        /***Downloads of Rtts from Germany***/
        JavaRDD<String> rttsGermany = inputNoHeader.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] split = s.split(",");
                //System.out.println(split[6]+" "+split[8]);
                if(split[6].equalsIgnoreCase("Rtts") && split[8].equalsIgnoreCase("DE")){
                    return true;
                }else
                    return false;
            }
        });
        //System.out.println(rttsGermany.collect());

        /***mapping table***/
        Map<String,String> countryTable = new HashMap<String,String>();
        countryTable.put("US","United States");
        countryTable.put("DE","Germany");
        countryTable.put("IN","India");
        countryTable.put("CN","China");

        System.out.println(countryTable);

    }
}
