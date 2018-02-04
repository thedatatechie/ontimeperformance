package com.emirates.di.offsets

import com.emirates.di.hbase.HBaseFun
import org.apache.kafka.common.TopicPartition

/**
  * Created by Vijaya.Ramisetti on 28/01/2018.
  */

case class TopicOffsets(topicPartition: TopicPartition, offset: Long) extends Serializable

object OffsetManager extends Serializable{

  //TODO remove the hard coaded values
  val cfName = "default"
  val colName = "offsets"

  def getLatestOffsets(tableName: String, topicPs: Set[TopicPartition], groupID: String): Set[TopicOffsets] = {
    topicPs.map(tp => {
      val rowKey = buildRowKey(tp, groupID)
      val result = HBaseFun.get(tableName, rowKey, cfName)
      if(!result.isEmpty) {
        val offsetResult = new String(result.getValue(cfName.getBytes(), colName.getBytes()))
        val offsetNum = offsetResult.toLong
        TopicOffsets(tp, offsetNum)
      }else{
        TopicOffsets(tp, 0L)
      }
    })
  }

  def saveOffsets(tableName: String, topicP: TopicPartition, offsetNum: Long, groupID: String) = {
    val rowKey = buildRowKey(topicP, groupID)
    HBaseFun.put(tableName, rowKey, cfName, colName, offsetNum.toString)
  }

  def buildRowKey(topicP: TopicPartition, groupID: String): String = {
    topicP.topic()+"-"+topicP.partition()+"-"+groupID
  }

}
