package com.dist.simplekafka

import java.util
import akka.actor.ActorSystem
import com.dist.simplekafka.common.TopicAndPartition
import com.dist.simplekafka.server.Config
import com.dist.simplekafka.util.ZkUtils.Broker

import java.util.concurrent.locks.ReentrantLock

class ReplicaManager(config:Config)(implicit actorSystem:ActorSystem) {
  val allPartitions = new util.HashMap[TopicAndPartition, Partition]()
  private val lock = new ReentrantLock()

  def makeFollower(topicAndPartition: TopicAndPartition, leader:Broker) = {
    lock.lock()
    try {
      val partition = getOrCreatePartition(topicAndPartition)
      partition.makeFollower(leader)
    } finally {
      lock.unlock()
    }
  }

  def makeLeader(topicAndPartition: TopicAndPartition) = {
    lock.lock()
    try {
      val partition = getOrCreatePartition(topicAndPartition)
      partition.makeLeader()
    } finally {
      lock.unlock()
    }
  }

  def getPartition(topicAndPartition: TopicAndPartition) = {
    lock.lock()
    try {
      allPartitions.get(topicAndPartition)
    } finally {
      lock.unlock()
    }
  }

  def getOrCreatePartition(topicAndPartition: TopicAndPartition) = {
    lock.lock()
    try {
      var partition = allPartitions.get(topicAndPartition)
      if (null == partition) {
        partition = new Partition(config, topicAndPartition)
        allPartitions.put(topicAndPartition, partition)
      }
      partition
    } finally {
      lock.unlock()
    }
  }
}
