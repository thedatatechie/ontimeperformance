package com.emirates.di

import com.emirates.di.config.Settings
import com.emirates.di.hbase.{HBaseFun}
import com.emirates.di.kafka.KafkaFun
import com.emirates.di.offsets.{OffsetManager, TopicOffsets}
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}


/**
  * Created by Vijaya.Ramisetti on 28/01/2018.
  */

object Main {
  @transient lazy val LOG = LogManager.getLogger(this.getClass)

  def getStreamingContext(dirPath: String, config: Settings): StreamingContext = {
    val kafkaParams = config.getKafkaParams
    val topics = config.getKafkaTopics
    val outputPath = config.getOutputPath
    val offsetTable = config.getOffsetTable
    val offsetTableCFList = config.getOffsetTableCFList
    val groupID = config.kafkaGroupId

    val driverMemory = config.getDriverMemory
    val executorMemory = config.getExecutorMemory
    val yarnMode = config.getYarnMode
    val appName = config.getAppName
    val batchInterval = config.getBatchInterval
    println("batch interval: "+batchInterval)

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("yarn")
      .set("spark.submit.deployMode", yarnMode)
      .set("spark.driver.memory", driverMemory)
      .set("spark.executor.memory", executorMemory)

    val session = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = session.sparkContext
    val ssc = new StreamingContext(sc, Seconds(batchInterval))
    ssc.checkpoint(dirPath)

    try {
      HBaseFun.dropTable(offsetTable)
      HBaseFun.createTable(offsetTable, offsetTableCFList)

      val topicsInfo = KafkaFun.getTopicPartitions(topics, kafkaParams)
      val latestOffsets: Set[TopicOffsets] = OffsetManager.getLatestOffsets(offsetTable, topicsInfo, groupID)

      val offsets = latestOffsets.map(topicOffset => {
        topicOffset.topicPartition -> topicOffset.offset
      }).toMap

      offsets.map(_.toString()).foreach(println)

      var rddOffsetRanges: Array[OffsetRange] = null

      val eventStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, offsets))
        .transform(rdd => {
          rddOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.mapPartitions(partition => {
            partition.map(_.value())
          })
        })

      eventStream.print(5)
      eventStream.saveAsTextFiles(outputPath+"/"+topics.head)

      eventStream.foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val offsets: OffsetRange = rddOffsetRanges(TaskContext.get.partitionId)
          val topicPartition = offsets.topicPartition()
          val partitionLastOffset = offsets.untilOffset
          OffsetManager.saveOffsets(offsetTable, topicPartition, partitionLastOffset, groupID)
        })
      })
    }catch{
      case ex: Exception => {
        ex.printStackTrace()
      }
    }

    ssc
  }

  def main(args: Array[String]): Unit = {

    var ssc: StreamingContext = null

    try {
      if (args.length != 3) {
        LOG.error("Invalid number of arguments. Must have three: config.properties topic_name outputpath")
        System.exit(-1)
      }

      val config = Settings(args)
      val checkPointDir = config.getCheckpointDir
      ssc = StreamingContext.getOrCreate(checkPointDir, () => getStreamingContext(checkPointDir, config))

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      ssc.start()
      LOG.info("streaming application is started.")
      ssc.awaitTermination()
    }
  }

}
