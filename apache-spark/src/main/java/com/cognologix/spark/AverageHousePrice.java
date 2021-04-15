package com.cognologix.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Create a Spark program to read the house data from in/RealEstate.csv,
 * output the average price for houses with different number of bedrooms, count of records for each number of bedroom
 * <p>
 * The houses dataset contains a collection of recent real estate listings in 
 * San Luis Obispo county and
 * around it. 
 * <p>
 * The dataset contains the following fields:
 * 1.MLS:Multiple listing service number for the house(unique ID).
 * 2.Location:city/town where the house is located. Most locations are in San Luis Obispo county and
 * northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
 * some out of area locations as well.
 * 3.Price: the most recent listing price of the house (in dollars).
 * 4.Bedrooms: number of bedrooms.
 * 5.Bathrooms: number of bathrooms.
 * 6.Size: size of the house in square feet.
 * 7.Price/SQ.ft: price of the house per square foot.
 * 8.Status:type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.
 * <p>
 * Each field is comma separated.
 * <p>
 * Sample output:
 * <p>
 * (3, 325000)
 * (1, 266356)
 * (2, 325000)
 * ...
 * <p>
 * 3, 1 and 2 mean the number of bedrooms.
 * 325000 means the average price of houses with 3 bedrooms is 325000.
 */

  /*
    Implement by using reduce operation on RDD

    Also implement this problem using Pair RDD.
   */


public class AverageHousePrice {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("AverageHousePrice");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        JavaRDD<String> ip = sc.textFile("apache-spark/in/RealEstate.csv");


        JavaRDD noHeader = ip.filter(x->!x.startsWith("MLS"));

        /***************************Returning avg prices for bedroom type Using JavaPairRDD and PairFunction***************************/

        PairFunction<String,Integer,Float> keyValue = new PairFunction<String, Integer, Float>() {
            @Override
            public Tuple2<Integer, Float> call(String s) throws Exception {
                return new Tuple2(Integer.parseInt(s.split(",")[3]),Float.parseFloat(s.split(",")[2]));
            }
        };

        JavaPairRDD<Integer,Float> bedroomsPrice = noHeader.mapToPair(keyValue);

        //System.out.println(bedroomsPrice.collect());


        JavaPairRDD<Integer,Iterable<Float>> bedroomGroup = bedroomsPrice.groupByKey();

        //System.out.println(bedroomGroup.collect());


        JavaPairRDD avgPrice = bedroomGroup.mapValues(floats -> {
            float sum = 0.0F;
            float avg = 0.0F;
            for (Float aFloat : floats) {
                sum +=aFloat;
            }

            avg=sum/floats.spliterator().getExactSizeIfKnown();

            return avg;
        });

        //System.out.println(avgPrice.collect());

        /*****************************Returning count of records for each bedroom type using reduceByKey****************/


        PairFunction<String,Integer,Tuple2<Integer,Float>> keyValueCount = new PairFunction<String, Integer, Tuple2<Integer, Float>>() {
            @Override
            public Tuple2<Integer, Tuple2<Integer, Float>> call(String s) throws Exception {
                int bedroom=Integer.parseInt(s.split(",")[3]);
                float price=Float.parseFloat(s.split(",")[2]);


                return new Tuple2(bedroom,new Tuple2(1,price));
            }
        };

        JavaPairRDD<Integer,Tuple2<Integer,Float>> countByBedroom = noHeader.mapToPair(keyValueCount);
        //System.out.println(countByBedroom.collect());

        JavaPairRDD<Integer, Tuple2<Integer, Float>> countPrice = countByBedroom.reduceByKey(new Function2<Tuple2<Integer, Float>, Tuple2<Integer, Float>, Tuple2<Integer, Float>>() {
            @Override
            public Tuple2<Integer, Float> call(Tuple2<Integer, Float> integerFloatTuple2, Tuple2<Integer, Float> integerFloatTuple22) throws Exception {

                return new Tuple2<>(integerFloatTuple2._1+integerFloatTuple22._1,integerFloatTuple2._2+integerFloatTuple22._2);

            }
        });
        System.out.println(countPrice.collect());
    }
}
