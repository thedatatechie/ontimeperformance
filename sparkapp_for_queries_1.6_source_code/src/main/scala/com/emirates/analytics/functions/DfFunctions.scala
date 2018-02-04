package com.emirates.analytics.functions

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{when, _}

/**
  * Created by Vijaya.Ramisetti on 28/01/2018.
  */
object DfFunctions {

  def getTheBestCarrier(spark: SparkSession, otpDF: Dataset[Row], carriersDF: Dataset[Row]): Row ={
    import spark.implicits._

    val carrUpdtDf = carriersDF
      .withColumnRenamed("Code", "UniqueCarrier")
      .withColumnRenamed("Description", "airlineDescription")

    otpDF.as('o)
      .join(carrUpdtDf.as('c), Seq("UniqueCarrier"), "left")
      .drop($"c.UniqueCarrier")
      .groupBy('UniqueCarrier)
      .agg(
        first('airlineDescription).as("airlineDescription"),
        count('TailNum).as("totalCount"),
        sum(when('ArrDelay >0, 1).otherwise(0)).as("numOfDelays"),
        sum(when('Cancelled > 0, 1).otherwise(0)).as("numOfCancellations"),
        sum(when('Diverted > 0, 1).otherwise(0)).as("numDiversions")
      )
      .withColumn("onTimePerformance", (('totalCount-('numOfDelays+'numOfCancellations+'numDiversions))*100/'totalCount).cast("float"))
      .orderBy(desc("onTimePerformance"))
      .first()
  }

  def getTheBestDay(spark:SparkSession, otpDF: Dataset[Row]): Row = {
    import spark.implicits._

    otpDF
      .groupBy('Year, 'Month, 'DayofMonth)
      .agg(
        count('TailNum).as("totalCount"),
        sum(when('ArrDelay >0, 1).otherwise(0)).as("numOfDelays"),
        sum(when('Cancelled > 0, 1).otherwise(0)).as("numOfCancellations"),
        sum(when('Diverted > 0, 1).otherwise(0)).as("numDiversions")
      )
      .withColumn("onTimePerformance", (('totalCount-('numOfDelays+'numOfCancellations+'numDiversions))*100/'totalCount).cast("float"))
      .orderBy(desc("onTimePerformance"))
      .first()
  }

  def getTheBestWeekDay(spark:SparkSession, otpDF: Dataset[Row]): Row = {
    import spark.implicits._

    otpDF
      .groupBy('DayOfWeek)
      .agg(
        count('TailNum).as("totalCount"),
        sum(when('ArrDelay >0, 1).otherwise(0)).as("numOfDelays"),
        sum(when('Cancelled > 0, 1).otherwise(0)).as("numOfCancellations"),
        sum(when('Diverted > 0, 1).otherwise(0)).as("numDiversions")
      )
      .withColumn("onTimePerformance", (('totalCount-('numOfDelays+'numOfCancellations+'numDiversions))*100/'totalCount).cast("float"))
      .orderBy(desc("onTimePerformance"))
      .first()
  }


  def getTheBestTime(spark:SparkSession, otpDF: Dataset[Row]): Row = {
    import spark.implicits._

    otpDF
      .withColumn("startingMin", ('CRSDepTime%100))
      .withColumn("startingHr", (('CRSDepTime-'startingMin)/100).cast("int"))
      .withColumn("nextHr", 'startingHr+1)
      .groupBy('startingHr, 'nextHr)
      .agg(
        count('TailNum).as("totalCount"),
        sum(when('ArrDelay >0, 1).otherwise(0)).as("numOfDelays"),
        sum(when('Cancelled > 0, 1).otherwise(0)).as("numOfCancellations"),
        sum(when('Diverted > 0, 1).otherwise(0)).as("numDiversions")
      )
      .withColumn("onTimePerformance", (('totalCount-('numOfDelays+'numOfCancellations+'numDiversions))*100/'totalCount).cast("float"))
      .orderBy(desc("onTimePerformance"))
      .first()
  }


  def getOldPlanesData(spark:SparkSession, otpDF: Dataset[Row], planeDF: Dataset[Row]): Array[Row] = {
    import spark.implicits._

    val planesUpdtDF = planeDF
      .select('tailnum, 'issue_date)
      .dropDuplicates()
      .withColumnRenamed("tailnum", "TailNum")

    otpDF.as('o)
      .join(planesUpdtDF.as('p), Seq("TailNum"), "left")
      .drop($"c.TailNum")
      .groupBy('TailNum)
      .agg(
        count('TailNum).as("totalCount"),
        sum(when('ArrDelay >0, 1).otherwise(0)).as("numOfDelays"),
        sum(when('Cancelled > 0, 1).otherwise(0)).as("numOfCancellations"),
        sum(when('Diverted > 0, 1).otherwise(0)).as("numDiversions"),
        first('issue_date).as("planeInductionDate")
      )
      .withColumn("onTimePerformance", (('totalCount-('numOfDelays+'numOfCancellations+'numDiversions))*100/'totalCount).cast("float"))
      .orderBy(asc("onTimePerformance"))
      .select('TailNum, 'planeInductionDate, 'onTimePerformance, 'totalCount)
      .head(100)
  }



  def getCascadingDelays(spark:SparkSession, otpDF: Dataset[Row], airportsDF: Dataset[Row]): Array[Row] = {
    import spark.implicits._

    val performanceWindow = Window.partitionBy().orderBy('originPerformance.asc)

    val airportsUpdtDF = airportsDF
      .select('iata, 'airport)
      .withColumn("Origin", 'iata)
      .withColumn("Dest", 'iata)
      .drop("iata")
      .dropDuplicates()

    val mainAirportsWithDelays = otpDF
      .groupBy('Dest)
      .agg(
        count('TailNum).as("totalCount"),
        sum(when('ArrDelay >0, 1).otherwise(0)).as("numOfDelays"),
        sum(when('Cancelled > 0, 1).otherwise(0)).as("numOfCancellations"),
        sum(when('Diverted > 0, 1).otherwise(0)).as("numDiversions")
      )
      .withColumn("onTimePerformance", (('totalCount-('numOfDelays+'numOfCancellations+'numDiversions))*100/'totalCount).cast("float"))
      .orderBy(desc("onTimePerformance"))
      .select('Dest, 'onTimePerformance)
      .withColumnRenamed("onTimePerformance", "originPerformance")
      .withColumnRenamed("Dest", "Origin")
      .dropDuplicates()
      .join(airportsUpdtDF.as('a), Seq("Origin"), "left")
      .drop($"a.Origin")
      .drop($"a.Dest")
      .withColumnRenamed("airport", "originAirport")
      .withColumn("rank", rank().over(performanceWindow))
      .filter('rank <= 100)


    otpDF.as('o)
      .join(mainAirportsWithDelays.as('a), Seq("Origin"), "inner")
      .drop($"a.Origin")
      .groupBy('Dest, 'Origin)
      .agg(
        first('originAirport).as("originAirport"),
        max('originPerformance).as("originPerformance"),
        count('TailNum).as("totalCount"),
        sum(when('ArrDelay >0, 1).otherwise(0)).as("numOfDelays"),
        sum(when('Cancelled > 0, 1).otherwise(0)).as("numOfCancellations"),
        sum(when('Diverted > 0, 1).otherwise(0)).as("numDiversions")
      )
      .withColumn("onTimePerformance", (('totalCount-('numOfDelays+'numOfCancellations+'numDiversions))*100/'totalCount).cast("float"))
      .select('Origin, 'originPerformance, 'originAirport, 'Dest, 'onTimePerformance)
      .dropDuplicates()
      .join(airportsUpdtDF.as('a), Seq("Dest"), "left")
      .drop($"a.Origin")
      .drop($"a.Dest")
      .withColumnRenamed("airport", "destAirport")
      .orderBy(asc("originPerformance"),asc("onTimePerformance"))
      .head(100)

  }


}
