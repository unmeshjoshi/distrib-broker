package com.dist.simplekafka

import java.util.concurrent.atomic.AtomicInteger

import com.dist.simplekafka.api.{RequestKeys, RequestOrResponse}
import com.dist.simplekafka.common.{JsonSerDes, Logging, TopicAndPartition}
import com.dist.simplekafka.network.InetAddressAndPort
import com.dist.simplekafka.util.Utils

object Coordinator {
  def findCoordinator(socketClient:SocketClient, bootstrapBroker: InetAddressAndPort, key:String, coordinatorType:String) = {
    var leaderId = -1
    var coordinatorResponse:FindCoordinatorResponse = null
    while(leaderId == -1) {
      val request = RequestOrResponse(RequestKeys.FindCoordinatorKey, JsonSerDes.serialize(FindCoordinatorRequest(key, coordinatorType)), 1)
      val response = socketClient.sendReceiveTcp(request, bootstrapBroker)
      val findCoordinatorResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[FindCoordinatorResponse])
      coordinatorResponse = findCoordinatorResponse
      leaderId = coordinatorResponse.partitionInfo.leader.id
    }
    coordinatorResponse
  }
}

class SimpleConsumer(bootstrapBroker: InetAddressAndPort, socketClient:SocketClient = new SocketClient) extends Logging {
  val correlationId = new AtomicInteger(0)
  val consumerId = "consumer1"
  val groupId = "default"

  def consume(topic: String, partition:Int) = {

  }

  var lastCommitedOffset = 0;
  def commitOffset(offset:Int)={
    val coordinatorResponse = Coordinator.findCoordinator(socketClient, bootstrapBroker, groupId, FindCoordinatorRequest.GROUP_COORDINATOR)
    val coordinator = coordinatorResponse.partitionInfo.leader
    val request = RequestOrResponse(RequestKeys.OffsetCommitRequest, JsonSerDes.serialize(OffsetCommitRequest(groupId, consumerId, offset, coordinatorResponse.topicPartition)), correlationId.getAndIncrement())
    val response = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(coordinator.host, coordinator.port))
    val offsetResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[FetchOffsetResponse])
    lastCommitedOffset = offsetResponse.offset
  }

  def consume(topic: String) = {
    val result = new java.util.HashMap[String, String]()
    val topicMetadata: Map[TopicAndPartition, PartitionInfo] = fetchTopicMetadata(topic)
    topicMetadata.foreach(tp ⇒ {
      val topicPartition = tp._1
      val partitionInfo = tp._2
      val leader = partitionInfo.leader
      val request = RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(ConsumeRequest(topicPartition)), correlationId.getAndIncrement())
      val response = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(leader.host, leader.port))
      val consumeResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[ConsumeResponse])
      consumeResponse.messages.foreach(m ⇒ {
        result.put(m._1, m._2)
      })
    })
    result
  }

  private def fetchTopicMetadata(topic: String) = {
    val request = RequestOrResponse(RequestKeys.GetMetadataKey, JsonSerDes.serialize(TopicMetadataRequest(topic)), 1)

    val response = socketClient.sendReceiveTcp(request, bootstrapBroker)
    val topicMetadataResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[TopicMetadataResponse])
    val topicPartitions: Map[TopicAndPartition, PartitionInfo] = topicMetadataResponse.topicPartitions
    topicPartitions
  }

  def partitionFor(key: String, numPartitions: Int) = {
    Utils.abs(key.hashCode) % numPartitions
  }

  def leaderFor(topicName: String, partitionId: Int, topicPartitions: Map[TopicAndPartition, PartitionInfo]) = {
    val info: PartitionInfo = topicPartitions(TopicAndPartition(topicName, partitionId))
    info.leader
  }
}
