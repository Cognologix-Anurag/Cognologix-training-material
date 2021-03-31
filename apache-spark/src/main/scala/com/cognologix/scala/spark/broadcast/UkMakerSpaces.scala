package com.cognologix.scala.spark.broadcast

/**
 * Create a Spark program to read the airport data from in/uk-makerspaces-identifiable-data.csv, and uk-postcode.csv
 * output the the list of counts by postal code
 *
 * Note :-  Column 1 data is from  uk-postcode.csv this needs to be mapped to postal code in uk-makerspaces-identifiable-data.csv
 * Rhondda Cynon Taf 1
 * null 2
 * Oxford 2
 * Berkshire 1
 * Canterbury 1
 *
 */
object UkMakerSpaces {

    /*
      Hint :- Approach - 1
        This programs can be completed using join operation where 2 RDDS will be there

        Try to write the program by using broadcast variable.

        Use this string to split the contents of the file.
        String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
     */



}
