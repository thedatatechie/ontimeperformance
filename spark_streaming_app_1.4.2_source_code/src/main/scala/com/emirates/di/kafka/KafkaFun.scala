package com.emirates.di.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConversions._

/**
  * Created by Vijaya.Ramisetti on 28/01/2018.
  */
object KafkaFun extends Serializable{

  def getTopicPartitions(topics: Set[String], kafkaParams: Map[String, String]): Set[TopicPartition] = {
    val consumer = new KafkaConsumer[String, String](kafkaParams)
    val topicsStrs: Set[TopicPartition] = topics.flatMap(topic => {
      val topicData = consumer.partitionsFor(topic)
      topicData.map(tp => {
        new TopicPartition(tp.topic(),tp.partition())
      })
    })

    consumer.close()
    topicsStrs
  }

}
