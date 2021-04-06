package com.cognologix.scala.spark.dataframe

/**
  Complete the below code
  https://docs.databricks.com/_static/notebooks/complex-nested-structured.html
*/
object ComplexNestedStructure {

  def main(args: Array[String]): Unit = {
    val eventDS = DataForComplexNestedStructure.eventsDS

    /*
    Step 1
    Create a Dataset using eventDS for the schema using this case class
    case class DeviceData (id: Int, device: String)
     */

    /*
    Step 2
    Create a Dataset using eventDS for the schema
        fieldname , datatype
        ("battery_level", LongType)
        ("c02_level", LongType)
        ("cca3",StringType)
        ("cn", StringType)
        ("device_id", LongType)
        ("device_type", StringType)
        ("signal", LongType)
        ("ip", StringType)
        ("temp", LongType)
        ("timestamp", TimestampType)

        In order to parse json and extract fields use - get_json_object , from_json API
     */


    /*
      Step 3
      Once you created the dataframe for the above schema filter
      devices.temp > 10 and devices.signal > 15
     */

    /*
      Step 4
      Convert the dataframe from Step 3 back to json string
      To convert dataframe fields into json strings use to_json API
     */



  }

}
