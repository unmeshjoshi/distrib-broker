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
    Utils.abs(key.hashCode) % TransactionLog.DefaultNumPartitions //TODO: Have different methods for group coordinator and txn coordinator
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
        val partitionInfo: PartitionInfo = if (findCoordinatorRequest.coordinatorType == FindCoordinatorRequest.GROUP_COORDINATOR) {
          getOrCreateInternalTopic(GROUP_METADATA_TOPIC_NAME, partition)
        } else if(findCoordinatorRequest.coordinatorType == FindCoordinatorRequest.TRANSACTION_COORDINATOR) {
          getOrCreateInternalTopic(TRANSACTION_STATE_TOPIC_NAME, partition)
        } else {
          throw new IllegalArgumentException("Invalid request");
        }
        RequestOrResponse(RequestKeys.FindCoordinatorKey, JsonSerDes.serialize(FindCoordinatorResponse(TopicAndPartition(GROUP_METADATA_TOPIC_NAME, partition), partitionInfo)), request.correlationId)
      }
      case RequestKeys.OffsetCommitRequestKey => {
        val offsetCommitRequest= JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[OffsetCommitRequest])
        val partition = replicaManager.getPartition(offsetCommitRequest.topicAndPartition)
        partition.append(s"${offsetCommitRequest.groupId}_${offsetCommitRequest.consumerId}", s"$offsetCommitRequest.offset")
        RequestOrResponse(RequestKeys.OffsetCommitRequestKey,
          JsonSerDes.serialize(FetchOffsetResponse(offsetCommitRequest.groupId, offsetCommitRequest.consumerId, offsetCommitRequest.offset)), request.correlationId)
      }
      case RequestKeys.InitProducerIdRequestKey => {
        val initProducerIdRequest: InitProducerIdRequest= JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[InitProducerIdRequest])
        val transactionCoordPartition = partitionFor(initProducerIdRequest.transactionalId)
        //TODO: Put this entry while handling leaderandisr request.
        if (transactionMetadataCache.get(transactionCoordPartition) == None) {
          transactionMetadataCache.put(transactionCoordPartition, TxnMetadataCacheEntry())
        }
        val maybeEntry = transactionMetadataCache.get(transactionCoordPartition)
        val time = System.currentTimeMillis();
        val producerId = 1 //TODO: Generate ProducerId
        val metadata = TransactionMetadata(initProducerIdRequest.transactionalId,
          producerId, 100, TransactionState.Empty, collection.mutable.Set.empty[TopicAndPartition], time, time)
        maybeEntry.flatMap(m => {
          m.metadataPerTransactionalId.put(initProducerIdRequest.transactionalId,
            metadata)
        })

        val partition = replicaManager.getOrCreatePartition(TopicAndPartition(TRANSACTION_STATE_TOPIC_NAME, transactionCoordPartition))
        partition.append(initProducerIdRequest.transactionalId, JsonSerDes.serialize(metadata))
        RequestOrResponse(RequestKeys.InitProducerIdRequestKey,
          JsonSerDes.serialize(InitProducerIdResponse(s"$producerId")), request.correlationId)
      }
      case RequestKeys.AddOffsetToTransactionKey => {
        val sendOffsetToTransactionRequest: AddOffsetToTransactionRequest= JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[AddOffsetToTransactionRequest])
        val offsetTopicPartition = new TopicAndPartition(GROUP_METADATA_TOPIC_NAME, partitionFor(sendOffsetToTransactionRequest.groupId))
        val transactionalId = sendOffsetToTransactionRequest.transactionalId
        val transactionCoordPartition = partitionFor(transactionalId)
        val cacheEntry = transactionMetadataCache.get(transactionCoordPartition).get
        val txnMetadata:TransactionMetadata = cacheEntry.metadataPerTransactionalId.get(transactionalId).get
        val txnTimestamp = System.currentTimeMillis();
        txnMetadata.prepareAddPartitions(List(offsetTopicPartition), txnTimestamp)
        val partition = replicaManager.getOrCreatePartition(TopicAndPartition(TRANSACTION_STATE_TOPIC_NAME, transactionCoordPartition))
        //add to transaction log
        partition.append(sendOffsetToTransactionRequest.transactionalId, JsonSerDes.serialize(txnMetadata))
        RequestOrResponse(RequestKeys.AddOffsetToTransactionKey,
          JsonSerDes.serialize(AddOffsetToTransactionResponse("")), request.correlationId)
      }
      case RequestKeys.AddPartitionsToTransactionKey => {
        val addPartitionsToTransaction = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[AddPartitionsToTransaction])
        val transactionalId = addPartitionsToTransaction.transactionalId
        val transactionCoordPartition = partitionFor(transactionalId)
        val cacheEntry = transactionMetadataCache.get(transactionCoordPartition).get
        val txnMetadata:TransactionMetadata = cacheEntry.metadataPerTransactionalId.get(transactionalId).get
        val txnTimestamp = System.currentTimeMillis();
        txnMetadata.prepareAddPartitions(addPartitionsToTransaction.partitions.toList, txnTimestamp)
        val partition = replicaManager.getOrCreatePartition(TopicAndPartition(TRANSACTION_STATE_TOPIC_NAME, transactionCoordPartition))
        //add to transaction log
        partition.append(transactionalId, JsonSerDes.serialize(txnMetadata))

        RequestOrResponse(RequestKeys.AddPartitionsToTransactionKey,
          JsonSerDes.serialize(AddPartitionsToTransactionResponse("")), request.correlationId)

      }
      case RequestKeys.EndTransactionKey => {
        val endTransactionRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[EndTransactionRequest])
        val transactionalId = endTransactionRequest.transactionalId
        val transactionCoordPartition = partitionFor(transactionalId)
        val cacheEntry = transactionMetadataCache.get(transactionCoordPartition).get
        RequestOrResponse(RequestKeys.EndTransactionKey,
          JsonSerDes.serialize(AddPartitionsToTransactionResponse("")), request.correlationId)
      }
      case _ ⇒ RequestOrResponse(0, "Unknown Request", request.correlationId)
    }
  }

  val transactionMetadataCache: mutable.Map[Int, TxnMetadataCacheEntry] = mutable.Map()
  private def getTopicMetadata(topicName: String) = {
    val topicAndPartitions = leaderCache.keySet().asScala.filter(topicAndPartition ⇒ topicAndPartition.topic == topicName)
    topicAndPartitions
  }

  val GROUP_METADATA_TOPIC_NAME = "__consumer_offsets"
  val TRANSACTION_STATE_TOPIC_NAME = "__transaction_state"

  private def getOrCreateInternalTopic(topic: String, partition:Int):PartitionInfo = {
    val topicMetadata: mutable.Set[TopicAndPartition] = getTopicMetadata(topic)
    if (topicMetadata.size == 0) {
      createInternalTopic(topic)
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
