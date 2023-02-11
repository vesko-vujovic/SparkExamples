package com.examples.spark

import org.apache.spark.sql.SparkSession

case class FlightData(FL_DATE: String,
                      OP_CARRIER: String,
                      OP_CARRIER_FL_NUM: Int,
                      ORIGIN: String,
                      DEST: String,
                      CRS_DEP_TIME: Int,
                      DEP_TIME: Double,
                      DEP_DELAY: Double,
                      TAXI_OUT: Double,
                      WHEELS_OFF: Double,
                      WHEELS_ON: Double,
                      TAXI_IN: Double,
                      CRS_ARR_TIME: Int,
                      ARR_TIME: Double,
                      ARR_DELAY: Double,
                      CANCELLED: Double,
                      CANCELLATION_CODE: Option[String],
                      DIVERTED: Double,
                      CRS_ELAPSED_TIME: Double,
                      ACTUAL_ELAPSED_TIME: Double,
                      AIR_TIME: Double,
                      DISTANCE: Double,
                      CARRIER_DELAY: Double,
                      WEATHER_DELAY: Double,
                      NAS_DELAY: Double,
                      SECURITY_DELAY: Double,
                      LATE_AIRCRAFT_DELAY: Double,
                      `Unnamed: 27`: Option[String])

case class Result(destination: String, `sum(ARR_DELAY)`: Double)

object DatasetVsDataFrames {

  def main(args: Array[String]): Unit = {

    val path500K = "./data/flight_delay/flight_data_500K.csv"
    val path750K = "./data/flight_delay/flight_data_750K.csv"
    val path1000K = "./data/flight_delay/flight_data_1000K.csv"
    val path1250K = "./data/flight_delay/flight_data_1250K.csv"
    val path1500K = "./data/flight_delay/flight_data_1500K.csv"
    val path1750K = "./data/flight_delay/flight_data_1750K.csv"
    val path2000K = "./data/flight_delay/flight_data_2000K.csv"
    val path2018 = "./data/flight_delay/2018.csv"

    val spark = SparkSession
      .builder()
      .master("spark://localhost:7077")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.memory.offHeap.enabled", true)
      .config("spark.memory.offHeap.size", "1g")
      .appName("DatasetsVsDataframes")
      .getOrCreate()

    val flight_data_df =
      spark.read
        .options(Map("header" -> "true"))
        .option("inferSchema", "true")
        .csv(path2018)
        .toDF()

    val summed = flight_data_df
      .groupBy("DEST")
      .sum("ARR_DELAY")
      .toDF("destination", "total_delay")
      .count()
//    import spark.implicits._
//
//    val flight_data_ds =
//      spark.read
//        .options(Map("header" -> "true"))
//        .option("inferSchema", "true")
//        .csv(path2018)
//        .as[FlightData]
//
//    val summed = flight_data_ds
//      .groupBy(col("DEST").as("destination"))
//      .sum("ARR_DELAY")
//      .as[Result]
//      .count()
  }
}
