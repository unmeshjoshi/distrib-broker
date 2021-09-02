package com.dist.simplekafka

import java.util.concurrent.atomic.AtomicInteger
import com.dist.simplekafka.api.{RequestKeys, RequestOrResponse}
import com.dist.simplekafka.common.{JsonSerDes, Logging, TopicAndPartition}
import com.dist.simplekafka.network.InetAddressAndPort
import com.dist.simplekafka.util.{Utils, ZkUtils}

import java.util

class SimpleProducer(bootstrapBroker: InetAddressAndPort, socketClient:SocketClient = new SocketClient) extends Logging {
  val correlationId = new AtomicInteger(0)
  private var newPartitionsInTransaction: util.Set[TopicAndPartition] = new util.HashSet[TopicAndPartition]();


  def produce(topic: String, key: String, message: String) = {
    val topicPartitions: Map[TopicAndPartition, PartitionInfo] = fetchTopicMetadata(topic)
    val partitionId = partitionFor(key, topicPartitions.size)
    val leaderBroker = leaderFor(topic, partitionId, topicPartitions)

    newPartitionsInTransaction.add(TopicAndPartition(topic, partitionId))

    val produceRequest = ProduceRequest(TopicAndPartition(topic, partitionId), key, message, transactionalId, producerId)
    val producerRequest = RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(produceRequest), correlationId.incrementAndGet())
    val produceResponse = socketClient.sendReceiveTcp(producerRequest, InetAddressAndPort.create(leaderBroker.host, leaderBroker.port))
    val response1 = JsonSerDes.deserialize(produceResponse.messageBodyJson.getBytes(), classOf[ProduceResponse])
    info(s"Produced message ${key} -> ${message} on leader broker ${leaderBroker}. Message offset is ${response1.offset}")
    response1.offset
  }

  import scala.jdk.CollectionConverters._

  def addPartitionsToTransaction(): Unit = {
    val coordinatorNode: ZkUtils.Broker = findTxnCoordinator
    val requestStr = JsonSerDes.serialize(AddPartitionsToTransaction(transactionalId, producerId, newPartitionsInTransaction.asScala.toSet))
    val request = RequestOrResponse(RequestKeys.AddPartitionsToTransactionKey, requestStr, correlationId.getAndIncrement())
    val response = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(coordinatorNode.host, coordinatorNode.port))
    val addPartitionResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[AddPartitionsToTransactionResponse])

  }

  def beginCommit(): Unit = {
    val coordinatorNode: ZkUtils.Broker = findTxnCoordinator
    val requestStr = JsonSerDes.serialize(EndTransactionRequest(transactionalId, producerId, true))
    val request = RequestOrResponse(RequestKeys.EndTransactionKey, requestStr, correlationId.getAndIncrement())
    val response = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(coordinatorNode.host, coordinatorNode.port))
    val endTransactionResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[EndTransactionResponse])
  }

  def commitTransaction(): Unit = {
    addPartitionsToTransaction()
    beginCommit();
  }

  private def fetchTopicMetadata(topic: String) = {
    val request = RequestOrResponse(RequestKeys.GetMetadataKey, JsonSerDes.serialize(TopicMetadataRequest(topic)), correlationId.getAndIncrement())

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


  val transactionalId = "producer1"
  var producerId:Long = RecordBatch.NO_PRODUCER_ID
  def initTransaction(): Unit = {
    val coordinatorNode: ZkUtils.Broker = findTxnCoordinator
    val request = createRequest(RequestKeys.InitProducerIdRequestKey, InitProducerIdRequest(transactionalId))
    val response = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(coordinatorNode.host, coordinatorNode.port))
    val initProducerIdResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[InitProducerIdResponse])
    producerId = initProducerIdResponse.producerId.toLong
  }

  private def createRequest(key: Short, request: Any) = {
    RequestOrResponse(key, JsonSerDes.serialize(request), correlationId.getAndIncrement())
  }

  def addOffsetsToTransaction(offsets: Map[TopicAndPartition, Int], groupId:String): Unit = {
    val coordinatorNode: ZkUtils.Broker = findTxnCoordinator
    val request = RequestOrResponse(RequestKeys.AddOffsetToTransactionKey, JsonSerDes.serialize(AddOffsetToTransactionRequest(offsets, groupId, transactionalId)), correlationId.getAndIncrement())
    val response = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(coordinatorNode.host, coordinatorNode.port))
    val addOffsetsToTransactionResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[InitProducerIdResponse])
  }

  private def findTxnCoordinator = {
    val response = Coordinator.findCoordinator(new SocketClient(), bootstrapBroker, transactionalId, FindCoordinatorRequest.TRANSACTION_COORDINATOR)
    val coordinatorNode = response.partitionInfo.leader
    coordinatorNode
  }
}
