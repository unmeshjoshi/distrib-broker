package com.dist.simplekafka

import com.dist.simplekafka.api.{RequestKeys, RequestOrResponse}
import com.dist.simplekafka.common.{JsonSerDes, TopicAndPartition}
import com.dist.simplekafka.server.Config
import com.dist.simplekafka.transaction.TransactionLog
import com.dist.simplekafka.util.{AdminUtils, Utils}
import com.dist.simplekafka.util.ZkUtils.Broker
import org.I0Itec.zkclient.exception.ZkNodeExistsException

import scala.collection.{Set, mutable}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._


class SimpleKafkaApi(config: Config, replicaManager: ReplicaManager) {
  var aliveBrokers:scala.collection.mutable.ListBuffer[Broker] = _
  var leaderCache = new java.util.HashMap[TopicAndPartition, PartitionInfo]
  val DefaultReplicaId = -1

  def isRequestFromReplica(consumeRequest: ConsumeRequest): Boolean = {
    consumeRequest.replicaId != DefaultReplicaId
  }

  def partitionFor(key: String)  = {
    Utils.abs(key.hashCode) % TransactionLog.DefaultNumPartitions
  }

  def handle(request: RequestOrResponse): RequestOrResponse = {
    request.requestId match {
      case RequestKeys.LeaderAndIsrKey => {
        val leaderAndReplicasRequest: LeaderAndReplicaRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[LeaderAndReplicaRequest])
        handleLeaderAndReplicas(leaderAndReplicasRequest.leaderReplicas)
        RequestOrResponse(RequestKeys.LeaderAndIsrKey, "", request.correlationId)
      }
      case RequestKeys.UpdateMetadataKey ⇒ {
        val updateMetadataRequest: UpdateMetadataRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[UpdateMetadataRequest])
        aliveBrokers = ListBuffer.from(updateMetadataRequest.aliveBrokers)
        updateMetadataRequest.leaderReplicas.foreach(leaderReplica ⇒ {
          println("Updating metadata for " + leaderReplica.topicPartition + " on broker" + config.brokerId)
          leaderCache.put(leaderReplica.topicPartition, leaderReplica.partitionStateInfo)
        })
        RequestOrResponse(RequestKeys.UpdateMetadataKey, "", request.correlationId)
      }
      case RequestKeys.GetMetadataKey ⇒ {
        val topicMetadataRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[TopicMetadataRequest])
        val topicAndPartitions = getTopicMetadata(topicMetadataRequest.topicName)
        val partitionInfo: Map[TopicAndPartition, PartitionInfo] = topicAndPartitions.map((tp: TopicAndPartition) ⇒ {
          (tp, leaderCache.get(tp))
        }).toMap
        val topicMetadata = TopicMetadataResponse(partitionInfo)
        RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(topicMetadata), request.correlationId)
      }
      case RequestKeys.ProduceKey ⇒ {
        val produceRequest: ProduceRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[ProduceRequest])
        val partition = replicaManager.getPartition(produceRequest.topicAndPartition)
        val offset = partition.append(produceRequest.key, produceRequest.message)
        RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(ProduceResponse(offset)), request.correlationId)
      }
      case RequestKeys.FetchKey ⇒ {
        val consumeRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[ConsumeRequest])
        val partition = replicaManager.getPartition(consumeRequest.topicAndPartition)

        val isolation = if (isRequestFromReplica(consumeRequest)) FetchLogEnd else FetchHighWatermark
        val rows = if (partition == null) List() else {
          partition.read(consumeRequest.offset, consumeRequest.replicaId, isolation)
        }

        if (isRequestFromReplica(consumeRequest) && partition != null) {
          partition.updateLastReadOffsetAndHighWaterMark(consumeRequest.replicaId, consumeRequest.offset)
        }

        val consumeResponse = ConsumeResponse(rows.map(row ⇒ (row.key, row.value)).toMap)
        RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(consumeResponse), request.correlationId)
      }
      case RequestKeys.FindCoordinatorKey => {
        val findCoordinatorRequest: FindCoordinatorRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[FindCoordinatorRequest])
        val partition = partitionFor(findCoordinatorRequest.key)
        val partitionInfo:PartitionInfo = getOrCreateInternalTopic(GROUP_METADATA_TOPIC_NAME, partition)
        RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(FindCoordinatorResponse(TopicAndPartition(GROUP_METADATA_TOPIC_NAME, partition), partitionInfo)), request.correlationId)
      }
      case RequestKeys.OffsetCommitRequest => {
        val offsetCommitRequest= JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[OffsetCommitRequest])
        val partition = replicaManager.getPartition(offsetCommitRequest.topicAndPartition)
        partition.append(s"${offsetCommitRequest.groupId}_${offsetCommitRequest.consumerId}", s"$offsetCommitRequest.offset")
        RequestOrResponse(RequestKeys.OffsetCommitRequest,
          JsonSerDes.serialize(FetchOffsetResponse(offsetCommitRequest.groupId, offsetCommitRequest.consumerId, offsetCommitRequest.offset)), request.correlationId)
      }
      case _ ⇒ RequestOrResponse(0, "Unknown Request", request.correlationId)
    }
  }

  private def getTopicMetadata(topicName: String) = {
    val topicAndPartitions = leaderCache.keySet().asScala.filter(topicAndPartition ⇒ topicAndPartition.topic == topicName)
    topicAndPartitions
  }

  val GROUP_METADATA_TOPIC_NAME = "__consumer_offsets"
  val TRANSACTION_STATE_TOPIC_NAME = "__transaction_state"

  private def getOrCreateInternalTopic(topic: String, partition:Int):PartitionInfo = {
    val topicMetadata: mutable.Set[TopicAndPartition] = getTopicMetadata(topic)
    if (topicMetadata.size == 0) {
      createInternalTopic(GROUP_METADATA_TOPIC_NAME)
      PartitionInfo(Broker(-1, "", -1), List())
    } else {
      leaderCache.get(TopicAndPartition(topic, partition));
    }
  }

  private def createInternalTopic(topic:String): Unit = {
    try {
      new CreateTopicCommand(zookeeperClient = new ZookeeperClientImpl(config)).createTopic(topic, TransactionLog.DefaultNumPartitions, TransactionLog.DefaultReplicationFactor)
    } catch {
      case e:ZkNodeExistsException => println("topic already exists, will wait for metadata to propogate")
    }
  }


  def handleLeaderAndReplicas(leaderReplicas:List[LeaderAndReplicas]) = {
    leaderReplicas.foreach(leaderAndReplicas ⇒ {
      val topicAndPartition = leaderAndReplicas.topicPartition
      val leader = leaderAndReplicas.partitionStateInfo.leader
      if (leader.id == config.brokerId)
        replicaManager.makeLeader(topicAndPartition)
      else
        replicaManager.makeFollower(topicAndPartition, leader)
    })
  }
}

case class Replica(val brokerId: Int,
                   val partition: Partition,
                   initialHighWatermarkValue: Long = 0L)
