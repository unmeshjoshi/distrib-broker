package com.dist.simplekafka

import com.dist.simplekafka.api.{RequestKeys, RequestOrResponse}
import com.dist.simplekafka.common.{JsonSerDes, Logging, TopicAndPartition}
import com.dist.simplekafka.network.InetAddressAndPort
import com.dist.simplekafka.server.Config
import com.dist.simplekafka.transaction.TransactionLog
import com.dist.simplekafka.util.Utils
import com.dist.simplekafka.util.ZkUtils.Broker
import org.I0Itec.zkclient.exception.ZkNodeExistsException

import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._



class SimpleKafkaApi(config: Config, replicaManager: ReplicaManager) extends Logging {
  var aliveBrokers: scala.collection.mutable.ListBuffer[Broker] = _
  var leaderCache = new java.util.HashMap[TopicAndPartition, PartitionInfo]
  val DefaultReplicaId = -1

  def isRequestFromReplica(consumeRequest: ConsumeRequest): Boolean = {
    consumeRequest.replicaId != DefaultReplicaId
  }

  def partitionFor(key: String) = {
    Utils.abs(key.hashCode) % TransactionLog.DefaultNumPartitions //TODO: Have different methods for group coordinator and txn coordinator
  }

  def sendEndTransactionMarker(leader:Broker, transactionalId:String, producerId:Long, topicAndPartition: TopicAndPartition) = {
    val request = RequestOrResponse(RequestKeys.WriteTxnsMarkerRequestKey,
      JsonSerDes.serialize(WriteTxnMarkersRequest(producerId, topicAndPartition, transactionalId, TransactionResult.COMMIT)), 0)
    new SocketClient().sendReceiveTcp(request,
        InetAddressAndPort.create(leader.host, leader.port))
  }

  val transactionMetadataCache: util.Map[Int, TxnMetadataCacheEntry] = new util.HashMap();
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
            println("Updating newStateMetadata for " + leaderReplica.topicPartition + " on broker" + config.brokerId)
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
          if (produceRequest.isTransactional()) {

          }
          val partition = replicaManager.getPartition(produceRequest.topicAndPartition)
          val offset = partition.append(produceRequest.transactionalId, produceRequest.producerId, produceRequest.key, produceRequest.message)
          waitUntilTrue(()=> offset == partition.highWatermark, "Waiting for message to replicate")
          RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(ProduceResponse(offset)), request.correlationId)
        }
        case RequestKeys.FetchKey ⇒ {
          val consumeRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[ConsumeRequest])
          val partition = replicaManager.getPartition(consumeRequest.topicAndPartition)

          //        val isolation = if (isRequestFromReplica(consumeRequest)) FetchLogEnd else FetchHighWatermark
          val isolation = if (isRequestFromReplica(consumeRequest)) {
            FetchLogEnd
          } else {
            if (consumeRequest.isolation == FetchTxnCommitted.toString) FetchTxnCommitted else FetchHighWatermark
          }
          val rows = if (partition == null) {
            List()
          } else {
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
          } else if (findCoordinatorRequest.coordinatorType == FindCoordinatorRequest.TRANSACTION_COORDINATOR) {
            getOrCreateInternalTopic(TRANSACTION_STATE_TOPIC_NAME, partition)
          } else {
            throw new IllegalArgumentException("Invalid request");
          }
          RequestOrResponse(RequestKeys.FindCoordinatorKey, JsonSerDes.serialize(FindCoordinatorResponse(TopicAndPartition(GROUP_METADATA_TOPIC_NAME, partition), partitionInfo)), request.correlationId)
        }
        case RequestKeys.OffsetCommitRequestKey => {
          val offsetCommitRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[OffsetCommitRequest])
          val partition = replicaManager.getPartition(offsetCommitRequest.topicAndPartition)
          partition.append(s"${offsetCommitRequest.groupId}_${offsetCommitRequest.consumerId}", s"$offsetCommitRequest.offset")
          RequestOrResponse(RequestKeys.OffsetCommitRequestKey,
            JsonSerDes.serialize(FetchOffsetResponse(offsetCommitRequest.groupId, offsetCommitRequest.consumerId, offsetCommitRequest.offset)), request.correlationId)
        }
        //transaction requests go to transaction coordinator
        case RequestKeys.InitProducerIdRequestKey => {
          val initProducerIdRequest: InitProducerIdRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[InitProducerIdRequest])
          val transactionCoordPartition = partitionFor(initProducerIdRequest.transactionalId)
          //the cache is populated when handling leaderAndIsr request.
          val maybeEntry: TxnMetadataCacheEntry = transactionMetadataCache.get(transactionCoordPartition)
          val time = System.currentTimeMillis();
          val producerId = 1 //TODO: Generate ProducerId
          val metadata = TransactionMetadata(initProducerIdRequest.transactionalId,
            producerId, RecordBatch.NO_PRODUCER_EPOCH, 100, Empty, collection.mutable.Set.empty[TopicAndPartition], time, time)

          val partition = replicaManager.getOrCreatePartition(TopicAndPartition(TRANSACTION_STATE_TOPIC_NAME, transactionCoordPartition))
          partition.append(initProducerIdRequest.transactionalId, JsonSerDes.serialize(metadata))
          maybeEntry.metadataPerTransactionalId.put(initProducerIdRequest.transactionalId,
            metadata)

          RequestOrResponse(RequestKeys.InitProducerIdRequestKey,
            JsonSerDes.serialize(InitProducerIdResponse(s"$producerId")), request.correlationId)
        }
        case RequestKeys.AddOffsetToTransactionKey => {
          val sendOffsetToTransactionRequest: AddOffsetToTransactionRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[AddOffsetToTransactionRequest])
          val offsetTopicPartition = new TopicAndPartition(GROUP_METADATA_TOPIC_NAME, partitionFor(sendOffsetToTransactionRequest.groupId))
          val transactionalId = sendOffsetToTransactionRequest.transactionalId
          val transactionCoordPartition = partitionFor(transactionalId)
          val cacheEntry = transactionMetadataCache.get(transactionCoordPartition)
          val txnMetadata: TransactionMetadata = cacheEntry.metadataPerTransactionalId.get(transactionalId)
          val txnTimestamp = System.currentTimeMillis();
          val newStateMetadata = txnMetadata.prepareAddPartitions(List(offsetTopicPartition), txnTimestamp)
          val partition = replicaManager.getOrCreatePartition(TopicAndPartition(TRANSACTION_STATE_TOPIC_NAME, transactionCoordPartition))

          //add to transaction log
          partition.append(sendOffsetToTransactionRequest.transactionalId, JsonSerDes.serialize(newStateMetadata))
          cacheEntry.metadataPerTransactionalId.put(transactionalId, newStateMetadata)

          RequestOrResponse(RequestKeys.AddOffsetToTransactionKey,
            JsonSerDes.serialize(AddOffsetToTransactionResponse("")), request.correlationId)
        }
        case RequestKeys.AddPartitionsToTransactionKey => {
          val addPartitionsToTransaction = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[AddPartitionsToTransaction])
          val transactionalId = addPartitionsToTransaction.transactionalId
          val transactionCoordPartition = partitionFor(transactionalId)
          val cacheEntry = transactionMetadataCache.get(transactionCoordPartition)
          val txnMetadata: TransactionMetadata = cacheEntry.metadataPerTransactionalId.get(transactionalId)
          val txnTimestamp = System.currentTimeMillis();
          val list = addPartitionsToTransaction.partitions.toList
          val newStateMetadata = txnMetadata.prepareAddPartitions(list, txnTimestamp)
          val partition = replicaManager.getOrCreatePartition(TopicAndPartition(TRANSACTION_STATE_TOPIC_NAME, transactionCoordPartition))
          //add to transaction log
          partition.append(transactionalId, JsonSerDes.serialize(newStateMetadata))
          cacheEntry.metadataPerTransactionalId.put(transactionalId, newStateMetadata)

          RequestOrResponse(RequestKeys.AddPartitionsToTransactionKey,
            JsonSerDes.serialize(AddPartitionsToTransactionResponse("")), request.correlationId)

        }
        case RequestKeys.EndTransactionKey => {
          try {
            val endTransactionRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[EndTransactionRequest])
            val transactionalId = endTransactionRequest.transactionalId
            val transactionCoordPartition = partitionFor(transactionalId)
            val cacheEntry: TxnMetadataCacheEntry = transactionMetadataCache.get(transactionCoordPartition)

            val partition = replicaManager.getOrCreatePartition(TopicAndPartition(TRANSACTION_STATE_TOPIC_NAME, transactionCoordPartition))
            val txnMetadata: TransactionMetadata = cacheEntry.metadataPerTransactionalId.get(transactionalId)
            //add to prepareToCommit state to transaction log
            val newStateMetadata = txnMetadata.prepareCommit()

            cacheEntry.metadataPerTransactionalId.put(transactionalId, newStateMetadata)
            partition.append(transactionalId, JsonSerDes.serialize(newStateMetadata))
            cacheEntry.metadataPerTransactionalId.put(transactionalId, newStateMetadata)

            cacheEntry.metadataPerTransactionalId.get(transactionalId).topicPartitions.foreach(tp => {
              try {
                val partitionInfo = getPartitionDetails(tp.topic)
                val leader: Broker = partitionInfo.get(tp).get.leader
                sendEndTransactionMarker(leader, transactionalId, endTransactionRequest.producerId, tp);
              } catch {
                case e:Exception => e.printStackTrace()
              }
              })

            val newStateMetadata1 = txnMetadata.completeCommit()
            cacheEntry.metadataPerTransactionalId.put(transactionalId, newStateMetadata1)
            partition.append(transactionalId, JsonSerDes.serialize(newStateMetadata1))
            cacheEntry.metadataPerTransactionalId.put(transactionalId, newStateMetadata1)


          } catch {
            case e: Exception => e.printStackTrace()
          }
          RequestOrResponse(RequestKeys.EndTransactionKey,
            JsonSerDes.serialize(AddPartitionsToTransactionResponse("")), request.correlationId)
        }
        case RequestKeys.WriteTxnsMarkerRequestKey => {
          val writeTxnMarkersRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[WriteTxnMarkersRequest])
          val partition = replicaManager.getPartition(writeTxnMarkersRequest.topicPartition)

          partition.completeTransaction(writeTxnMarkersRequest)

          RequestOrResponse(RequestKeys.WriteTxnsMarkerRequestKey,
            JsonSerDes.serialize(AddPartitionsToTransactionResponse("")), request.correlationId)
        }
        case _ ⇒ RequestOrResponse(0, "Unknown Request", request.correlationId)
      }
   }

  private def getPartitionDetails(name: String) = {
    val topicAndPartitions = getTopicMetadata(name)
    val partitionInfo: Map[TopicAndPartition, PartitionInfo] = topicAndPartitions.map((tp: TopicAndPartition) ⇒ {
      (tp, leaderCache.get(tp))
    }).toMap
    partitionInfo
  }

  private def getTopicMetadata(topicName: String) = {
    val topicAndPartitions = leaderCache.keySet().asScala.filter(topicAndPartition ⇒ topicAndPartition.topic == topicName)
    topicAndPartitions
  }

  val GROUP_METADATA_TOPIC_NAME = "__consumer_offsets"
  val TRANSACTION_STATE_TOPIC_NAME = "__transaction_state"

  private def getOrCreateInternalTopic(topic: String, partition: Int): PartitionInfo = {
    val topicMetadata: mutable.Set[TopicAndPartition] = getTopicMetadata(topic)
    if (topicMetadata.size == 0) {
      createInternalTopic(topic)
      PartitionInfo(Broker(-1, "", -1), List())
    } else {
      leaderCache.get(TopicAndPartition(topic, partition));
    }
  }

  private def createInternalTopic(topic: String): Unit = {
    try {
      new CreateTopicCommand(zookeeperClient = new ZookeeperClientImpl(config)).createTopic(topic, TransactionLog.DefaultNumPartitions, TransactionLog.DefaultReplicationFactor)
    } catch {
      case e: ZkNodeExistsException => println("topic already exists, will wait for newStateMetadata to propogate")
      case e:Exception => println(e.getMessage)
    }
  }


  class TransactionStateManager(config: Config, val transactionMetadataCache: util.Map[Int, TxnMetadataCacheEntry]) {


    def loadTransactionsForTxnTopicPartition(partitionId: Int,
                                             addTxnMarkersToSend: (Int, TransactionResult, TransactionMetadata, TxnTransitMetadata) => Unit) = {

      val topicPartition = new TopicAndPartition(TRANSACTION_STATE_TOPIC_NAME, partitionId)
      val loadedTransactions: util.Map[String, TransactionMetadata] = new util.HashMap() //TODO: Load from the log.
      addLoadedTransactionsToCache(topicPartition.partition,
        RecordBatch.NO_PRODUCER_EPOCH,
        loadedTransactions)
    }

    def addLoadedTransactionsToCache(txnTopicPartition: Int,
                                     coordinatorEpoch: Long,
                                     txns: util.Map[String, TransactionMetadata]) = {

      val txnMetadataCacheEntry = TxnMetadataCacheEntry(txns)
      val previousTxnMetadataCacheEntry = transactionMetadataCache.put(txnTopicPartition, txnMetadataCacheEntry)
      println(s"Unloaded transaction newStateMetadata $previousTxnMetadataCacheEntry from $txnTopicPartition as part of " +
        s"loading newStateMetadata at epoch $coordinatorEpoch")
    }
  }


    class TransactionMarkerChannelManager(config: Config) {
      def addTxnMarkersToSend(coordinatorEpoch: Int,
                              txnResult: TransactionResult,
                              txnMetadata: TransactionMetadata,
                              newMetadata: TxnTransitMetadata): Unit = {

      }
    }

    class TransactionCoordinator(config: Config, val transactionMetadataCache: util.Map[Int, TxnMetadataCacheEntry]) {
      val txnStateManager = new TransactionStateManager(config, transactionMetadataCache)
      val txnMarkerChannelManager = new TransactionMarkerChannelManager(config)

      def onElection(txnTopicPartitionId: Int) = {
        //add coordinator epoch support
        println(s"Elected as the txn coordinator for partition $txnTopicPartitionId at epoch 0")
        txnStateManager.loadTransactionsForTxnTopicPartition(txnTopicPartitionId, txnMarkerChannelManager.addTxnMarkersToSend)
      }
    }


  val txnCoordinator = new TransactionCoordinator(config, transactionMetadataCache)

  def handleLeaderAndReplicas(leaderReplicas:List[LeaderAndReplicas]) = {
    info(s"Handling LeaderAndISR Request in ${config.brokerId} " + leaderReplicas)
    leaderReplicas.foreach(leaderAndReplicas ⇒ {
      val topicAndPartition = leaderAndReplicas.topicPartition
      val leader = leaderAndReplicas.partitionStateInfo.leader
      if (leader.id == config.brokerId) {
        replicaManager.makeLeader(topicAndPartition)
        if (topicAndPartition.topic == TRANSACTION_STATE_TOPIC_NAME) {
          txnCoordinator.onElection(topicAndPartition.partition);
        }
      }
      else {
        replicaManager.makeFollower(topicAndPartition, leader)
      }
    })
  }

    case class Replica(val brokerId: Int,
                       val partition: Partition,
                       initialHighWatermarkValue: Long = 0L)

  def waitUntilTrue(condition: () => Boolean, msg: => String,
                    waitTimeMs: Long = 1000, pause: Long = 100L): Unit = {
    val startTime = System.currentTimeMillis()
    while (true) {
      if (condition())
        return
      if (System.currentTimeMillis() > startTime + waitTimeMs)
        throw new RuntimeException(msg)

      Thread.sleep(waitTimeMs.min(pause))
    }

    // should never hit here
    throw new RuntimeException("unexpected error")
  }
}