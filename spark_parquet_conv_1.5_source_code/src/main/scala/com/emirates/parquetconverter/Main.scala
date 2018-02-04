package com.emirates.parquetconverter

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.databricks.spark.avro._

/**
  * Created by Vijaya.Ramisetti on 29/01/2018.
  */
class Main {

  def run(args: Array[String]): Unit = {

    lazy val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    import spark.sqlContext.implicits._

    val config = Config.parse(args)
    val inputPath = config.inputLoc
    val outputPath = config.outputLoc

    val df = spark
      .read
      .avro(inputPath)
      .dropDuplicates()

    val header = df.first()
    val dfUpdt = df.filter(row => row != header)
    dfUpdt.write.mode(SaveMode.Overwrite).parquet(outputPath)

  }

}


object Main {

  def main(args: Array[String]): Unit = {
    new Main().run(args)
  }

}