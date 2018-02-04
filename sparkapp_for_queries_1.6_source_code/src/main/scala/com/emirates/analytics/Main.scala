package com.emirates.analytics

import org.apache.spark.sql.SparkSession
import com.emirates.analytics.functions.DfFunctions._

/**
  * Created by Vijaya.Ramisetti on 28/01/2018.
  */

class Main {
  def run(args: Array[String]): Unit = {

    lazy val spark: SparkSession = SparkSession
      .builder()
      .config("spark.sql.crossJoin.enabled", value = true)
      .getOrCreate()

    val config = Config.parse(args)
    val otpDataPath = config.otpLoc
    val airportsDataPath = config.airportsLoc
    val carriersDataPath = config.carriersLoc
    val planedateDataPath = config.planedateLoc

    val getData = {path: String => spark
                                  .read
                                  .parquet(path)
                                  .dropDuplicates()
                                  }


    //Reads the required data
    val otpData = getData(otpDataPath)
    val airportsData = getData(airportsDataPath)
    val carriersData = getData(carriersDataPath)
    val planedateData = getData(planedateDataPath)


    // Logic to execute the,	Which carrier performs better?
    val theBestCarrier = getTheBestCarrier(spark, otpData, carriersData)
    val bestCarrier = theBestCarrier.getAs[String]("airlineDescription")
    val bestPerformance = theBestCarrier.getAs[Float]("onTimePerformance")
    var planeCount = theBestCarrier.getAs[Float]("totalCount")
    println("The Best carrier is: "+bestCarrier+" with on-time-performance of "+"%.2f".format(bestPerformance)+ " with %d number of flight landings".format(planeCount))


    //When is the best time of day to fly to minimise delays?
    val theBestTime = getTheBestTime(spark, otpData)
    val startHr = theBestTime.getAs[Int]("startingHr")
    val endHr = theBestTime.getAs[Int]("nextHr")
    val bestPerformanceTime = theBestTime.getAs[Float]("onTimePerformance")
    planeCount = theBestCarrier.getAs[Float]("totalCount")
    println("The Best Time to travel is between "+startHr+"Hours and "+endHr+"Hours with on-time-performance of "+"%.2f".format(bestPerformanceTime)+ " with %d number of flight landings".format(planeCount))

    //•	When is the best day of week to fly to minimise delays?
    val theBestWeekDay = getTheBestWeekDay(spark, otpData)
    val dayOfWeek = theBestWeekDay.getAs[Int]("DayOfWeek")
    val bestPerformanceWeekDay = theBestTime.getAs[Float]("onTimePerformance")
    planeCount = theBestCarrier.getAs[Float]("totalCount")
    println("the Best day to travel is "+dayOfWeek+ "with on-time-performance of "+"%.2f".format(bestPerformanceWeekDay)+ " with %d number of flight landings".format(planeCount))

    //•	When is the best time of year to fly to minimise delays?
    val theBestDay = getTheBestDay(spark, otpData)
    val year = theBestDay.getAs[Int]("Year")
    val month = theBestDay.getAs[Int]("Month")
    val dayOfMonth = theBestDay.getAs[Int]("DayofMonth")
    val bestPerformanceDay = theBestTime.getAs[Float]("onTimePerformance")
    planeCount = theBestCarrier.getAs[Float]("totalCount")
    println("the Best day to travel is "+dayOfMonth+"-"+month+"-"+year+ "with on-time-performance of "+"%.2f".format(bestPerformanceDay)+ " with %d number of flight landings".format(planeCount))


    //•	Do older planes suffer more delays?
    val worstPerformingPlanes = getOldPlanesData(spark, otpData, planedateData)
    println("The List of worst performing planes.")
    worstPerformingPlanes.foreach(println)


    //•	Can you detect cascading failures as delays in one airport create delays in others? Are there critical links in the system?
    val airportCascadingEffect = getCascadingDelays(spark, otpData, airportsData)
    println("The List of worst performing aiports with cascading effects.")
    airportCascadingEffect.foreach(println)


  }
}


object Main {

  def main(args: Array[String]): Unit = {
    new Main().run(args)
  }

}
