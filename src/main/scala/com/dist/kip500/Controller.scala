package com.dist.kip500

class Controller(log: Array[Byte]) {
  def createTopic(topic: String, noOfPartitions: Int): Unit = {
    val topicRecord = TopicRecord(topic, "1")
    val partitionRecord = PartitionRecord("0", topic, List(0, 1), 0)
  }

  def brokerHeartbeat() {}

}
