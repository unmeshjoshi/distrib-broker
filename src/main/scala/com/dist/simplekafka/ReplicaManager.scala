package com.dist.simplekafka

import java.util

import akka.actor.ActorSystem
import com.dist.simplekafka.common.TopicAndPartition
import com.dist.simplekafka.server.Config
import com.dist.simplekafka.util.ZkUtils.Broker

class ReplicaManager(config:Config)(implicit actorSystem:ActorSystem) {
  val allPartitions = new util.HashMap[TopicAndPartition, Partition]()

  def makeFollower(topicAndPartition: TopicAndPartition, leader:Broker) = {
    val partition = getOrCreatePartition(topicAndPartition)
    partition.makeFollower(leader)
  }

  def makeLeader(topicAndPartition: TopicAndPartition) = {
    val partition = getOrCreatePartition(topicAndPartition)
    partition.makeLeader()
  }

  def getPartition(topicAndPartition: TopicAndPartition) = {
    allPartitions.get(topicAndPartition)
  }

  def getOrCreatePartition(topicAndPartition: TopicAndPartition) = {
    var partition = allPartitions.get(topicAndPartition)
    if (null == partition) {
      partition = new Partition(config, topicAndPartition)
      allPartitions.put(topicAndPartition, partition)
    }
    partition
  }

}
