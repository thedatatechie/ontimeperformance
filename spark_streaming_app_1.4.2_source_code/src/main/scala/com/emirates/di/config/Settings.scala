package com.emirates.di.config

import com.emirates.di.hbase.HBaseFun
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.TableNotFoundException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.log4j.LogManager

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by Vijaya.Ramisetti on 28/01/2017.
  */

class Settings(config: PropertiesConfiguration) extends Serializable{
  @transient lazy val LOG = LogManager.getLogger(this.getClass)

  def getCheckpointDir: String = {
    config.getString(Settings.CHECKPOINT_DIR)
  }

  def getKafkaTopics: Set[String] = {
    val topicsStr = config.getStringArray(Settings.TOPIC_NAME)
    topicsStr.toSet
  }

  def getOutputPath: String = {
    val topicsStr = config.getString(Settings.OUTPUT_PATH)
    topicsStr
  }

  def getOffsetTable: String = {
    config.getString("hbase.offsetTable")
  }

  def getOffsetTableCFList: Set[String] = {
    val cfList = config.getString("hbase.offsetTable.cfNames")
    cfList.split(",").toSet
  }

  def getBatchInterval: Int = {
    val bi = Try(config.getInt("batch.interval"))
    val interval = if(bi.isSuccess){
      bi.get
    }else{
      LOG.error("The batch interval is not specified. Default batch interval of 10sec will be considered.")
      10
    }
    interval
  }

  def getKafkaParams: Map[String, String] = {
    Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> kafkaGroupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> kafkaKeyDeserializer,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> kafkaValueDeserializer,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> getConsumerAutoCommit
    )
  }

  def getConsumerAutoCommit: String = {
    config.getString("enable.auto.commit")
  }

  def kafkaBootstrapServers: String = {
    config.getString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
  }

  def kafkaGroupId: String = {
    config.getString("group.id")
  }

  def kafkaKeyDeserializer: String = {
    config.getString(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
  }

  def kafkaValueDeserializer: String = {
    config.getString(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
  }

  def sparkKafkaConsumerPollTimeout: String = {
    config.getString(Settings.SPARK_KAFKA_POLL_TIMEOUT)
  }

  def getDriverMemory: String = {
    config.getString("driver.memory")
  }

  def getExecutorMemory: String = {
    config.getString("executor.memory")
  }

  def getYarnMode: String = {
    config.getString("yarn.mode")
  }

  def getAppName: String = {
    config.getString("app.name")
  }

  def checkTopicPresent(topicName: String): Unit = {
    try {
      val consumer = new KafkaConsumer[String, String](getKafkaParams)
      val topics = consumer.listTopics()
      val status = topics.keySet().contains(topicName)
      if(!status){
        LOG.error(String.format(s"Could not find the topic $topicName"))
        System.exit(-1)
      }
    }catch {
      case ex : TableNotFoundException => {
        LOG.error(String.format(s"Could not find the topic $topicName"))
        System.exit(-1)
      }
    }
  }

}

object Settings extends Serializable{
  @transient lazy val LOG = LogManager.getLogger(this.getClass)
  val CHECKPOINT_DIR = "checkpoint.dir"
  val CARRIERS_TOPIC = "kafka.carriers.topic"
  val OTP_TOPIC = "otp_kafka"
  val SPARK_KAFKA_POLL_TIMEOUT = "spark.streaming.kafka.consumer.poll.ms"
  var TOPIC_NAME: String = null
  var OUTPUT_PATH: String = null

  def apply(argsList: Array[String]): Settings = {
    val config = new PropertiesConfiguration(argsList(0))
    TOPIC_NAME = argsList(1)
    OUTPUT_PATH = argsList(2)

    try {
      new Settings(config)
    }catch{
      case ex:Exception => {
        LOG.error("Error in loading config file.")
        ex.printStackTrace()
        throw new RuntimeException(ex)
      }
    }
  }
}
