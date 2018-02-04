package com.emirates.analytics.functions

import java.sql.{Date}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import com.emirates.analytics.functions.Context._
import com.emirates.analytics.functions.DfFunctions._


/**
  * Created by Vijaya.Ramisetti on 29/01/2018.
  */
@RunWith(classOf[JUnitRunner])
class DfFunctionsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  "test the best carrier "should " match the expected" in {
    import spark.implicits._

    val input_oat = Seq(
      //      ("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime"),
      ("N104UW", "WN", 20, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE", -4, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE",  0, 0, 0, 2008, 2, 25, 6, 1650),

      ("N104UW", "WN", 20, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE", -4, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE", 10, 0, 0, 2008, 2, 25, 6, 1650),

      ("N104UW", "WN",  0, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE",  1, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE", 10, 0, 0, 2008, 2, 25, 6, 1650)
    ).toDF("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Diverted", "Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime")

    val input_carriers = Seq(
      //      ("Code", "Description")
      ("WN", "Southwest Airlines Co"),
      ("WE", "Centurion Cargo Inc")
    ).toDF("Code", "Description")

    val input_planes = Seq(
      ("N104UW", Date.valueOf("2004-11-16")),
      ("N105UW", Date.valueOf("2005-11-16")),
      ("N106UW", Date.valueOf("2006-11-16")),
      ("N107UW", Date.valueOf("2007-11-16"))
    ).toDF("tailnum", "issue_date")

    val getResult = getTheBestCarrier(spark, input_oat, input_carriers)
    val carrier = getResult.getAs[String]("UniqueCarrier")

    val expected = "WE"

    carrier should be(expected)
  }

  "test the best day "should " match the expected" in {
    import spark.implicits._

    val input_oat = Seq(
      //      ("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime"),
      ("N104UW", "WN", 20, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE", -4, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE",  0, 0, 0, 2008, 2, 25, 6, 1650),

      ("N104UW", "WN", 20, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE", -4, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE", 10, 0, 0, 2008, 2, 25, 6, 1650),

      ("N104UW", "WN",  0, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE",  1, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE", 10, 0, 0, 2008, 2, 25, 6, 1650)
    ).toDF("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Diverted", "Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime")


    val getResult = getTheBestDay(spark, input_oat)
    val year = getResult.getAs[Int]("Year")
    val month = getResult.getAs[Int]("Month")
    val dayOfMonth = getResult.getAs[Int]("DayofMonth")

    val expectedYear = 2008
    val expectedMonth = 2
    val expectedDay = 4

    year should be (expectedYear)
    month should be (expectedMonth)
    dayOfMonth should be (expectedDay)
  }


  "test the best week day "should " match the expected" in {
    import spark.implicits._

    val input_oat = Seq(
      //      ("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime"),
      ("N104UW", "WN", 20, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE", -4, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE",  0, 0, 0, 2008, 2, 25, 6, 1650),

      ("N104UW", "WN", 20, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE", -4, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE", 10, 0, 0, 2008, 2, 25, 6, 1650),

      ("N104UW", "WN",  0, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE",  1, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE", 10, 0, 0, 2008, 2, 25, 6, 1650)
    ).toDF("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Diverted", "Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime")

    val getResult = getTheBestWeekDay(spark, input_oat)
    val dayOfWeek = getResult.getAs[Int]("DayOfWeek")
    val expectedDay = 2

    dayOfWeek should be (expectedDay)
  }


  "test the best time slot "should " match the expected" in {
    import spark.implicits._

    val input_oat = Seq(
      //      ("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime"),
      ("N104UW", "WN", 20, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE", -4, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE",  0, 0, 0, 2008, 2, 25, 6, 1650),

      ("N104UW", "WN", 20, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE", -4, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE", 10, 0, 0, 2008, 2, 25, 6, 1650),

      ("N104UW", "WN",  0, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE",  1, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE", 10, 0, 0, 2008, 2, 25, 6, 1650)
    ).toDF("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Diverted", "Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime")

    val getResult = getTheBestTime(spark, input_oat)
    val startHr = getResult.getAs[Int]("startingHr")
    val endHr = getResult.getAs[Int]("nextHr")

    val expectedStartHr = 21
    val expectedEndHr = 22

    startHr should be (expectedStartHr)
    endHr should be (expectedEndHr)
  }


  "test the Worst planes "should " match the expected" in {
    import spark.implicits._

    val input_oat = Seq(
      //      ("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime"),
      ("N104UW", "WN", 20, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE", -4, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE",  0, 0, 0, 2008, 2, 25, 6, 1650),

      ("N104UW", "WN", 20, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE", -4, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE", 10, 0, 0, 2008, 2, 25, 6, 1650),

      ("N104UW", "WN",  0, 0, 0, 2008, 2, 25, 3, 1920),
      ("N105UW", "WN", 55, 0, 0, 2008, 1, 18, 6, 1615),
      ("N106UW", "WE",  1, 0, 0, 2008, 2, 4, 2, 2145),
      ("N107UW", "WE", 10, 0, 0, 2008, 2, 25, 6, 1650)
    ).toDF("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Diverted", "Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime")

    val input_planes = Seq(
      ("N104UW", Date.valueOf("2004-11-16")),
      ("N105UW", Date.valueOf("2005-11-16")),
      ("N106UW", Date.valueOf("2006-11-16")),
      ("N107UW", Date.valueOf("2007-11-16"))
    ).toDF("tailnum", "issue_date")

    val getResult = getOldPlanesData(spark, input_oat, input_planes)
    getResult.foreach(println)

    val exp_seq = Seq(
      ("N105UW",Date.valueOf("2005-11-16"),0.0,3),
      ("N107UW",Date.valueOf("2007-11-16"),33.333332,3),
      ("N104UW",Date.valueOf("2004-11-16"),33.333332,3),
      ("N106UW",Date.valueOf("2006-11-16"),66.666664,3)
    ).toDF("TailNum", "planeInductionDate", "onTimePerformance", "totalCount").collect()

    getResult(0).getAs[String]("TailNum") should be ("N105UW")
    getResult(0).getAs[Date]("planeInductionDate") should be (Date.valueOf("2005-11-16"))
    getResult(0).getAs[Float]("onTimePerformance") === (0.0+-0.001)
    getResult.length should be (exp_seq.length)
  }


  "test the Worst aiports "should " match the expected" in {
    import spark.implicits._

    val input_oat = Seq(
      ("N104UW", "WN", 10, 0, 0, "IAD", "IND"),
      ("N105UW", "WN", 10, 0, 0, "IND", "LAS"),
      ("N106UW", "WE",  0, 0, 0, "IND", "MCO"),

      ("N104UW", "WN", 10, 0, 0, "IAD", "IND"),
      ("N105UW", "WN", 10, 0, 0, "IND", "LAS"),
      ("N106UW", "WE", 10, 0, 0, "IND", "MCO"),

      ("N104UW", "WN", 10, 0, 0, "IAD", "IND"),
      ("N105UW", "WN", 10, 0, 0, "IND", "LAS"),
      ("N107UW", "WE", 10, 0, 0, "IND", "MCO")

    ).toDF("TailNum", "UniqueCarrier", "ArrDelay", "Cancelled", "Diverted", "Origin", "Dest")


    val input_airports = Seq(
      ("IAD", "Washington Dulles International"),
      ("IND", "Indianapolis International"),
      ("LAS", "McCarran International"),
      ("MCO", "Orlando International")
    ).toDF("iata", "airport")

    val getResult = getCascadingDelays(spark, input_oat, input_airports)

    getResult(0).getAs[String]("Origin") should be ("IND")
    getResult(0).getAs[String]("originAirport") should be ("Indianapolis International")
    getResult(0).getAs[Float]("originPerformance") === (0.0+-0.001)
    getResult(0).getAs[String]("Dest") should be ("LAS")
    getResult(0).getAs[String]("destAirport") should be ("McCarran International")
    getResult(0).getAs[Float]("onTimePerformance") === (0.0+-0.001)
    getResult.length should be (2)
  }

}
