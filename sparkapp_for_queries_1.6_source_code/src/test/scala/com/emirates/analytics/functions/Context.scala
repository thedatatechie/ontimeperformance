package com.emirates.analytics.functions

import org.apache.spark.sql.SparkSession

/**
  * Created by Vijaya.Ramisetti on 29/01/2018.
  */
object Context {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("OA Algorithm Tests")
    .config("spark.sql.crossJoin.enabled", value = true)
    .getOrCreate()

}
