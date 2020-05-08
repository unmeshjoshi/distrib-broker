package org.dist.simplekafka.network

import java.util.concurrent.atomic.AtomicInteger

import org.dist.simplekafka.api.{RequestKeys, RequestOrResponse, TopicMetadataRequest, TopicMetadataResponse}
import org.dist.simplekafka.common.JsonSerDes
import org.dist.simplekafka.server.Config

import scala.collection.Set

class Client(bootstrapBroker:InetAddressAndPort, config:Config) {
  val correlationId = new AtomicInteger(0)
  val clientId = "client1"
  val socketClient = new SocketSender


  def fetchTopicMetadata(topics: Set[String]): Unit = {
    val correlationIdForRequest = correlationId.getAndIncrement()
    val topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, correlationIdForRequest, clientId, topics.toSeq)
    val response = socketClient.sendReceiveTcp(new RequestOrResponse(RequestKeys.GetMetadataKey, JsonSerDes.serialize(topicMetadataRequest), correlationIdForRequest), bootstrapBroker)
     val topicMetadataResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[TopicMetadataResponse])
    print(topicMetadataResponse.topicsMetadata)
  }
}
