package com.dist.simplekafka

import java.io.ByteArrayInputStream
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.dist.simplekafka.api.RequestOrResponse
import com.dist.simplekafka.common.{JsonSerDes, Logging, TopicAndPartition}
import com.dist.simplekafka.kip500.{BrokerHeartbeat, EntryType, FenceBroker, FetchRequest, FetchResponse, Lease, PartitionRecord, Record, SetValueRecord, TopicRecord, WalEntry}
import com.dist.simplekafka.network.InetAddressAndPort
import com.dist.simplekafka.server.Config
import com.dist.simplekafka.util.ZkUtils.Broker

import scala.collection.mutable.ListBuffer

class Server(val config:Config, val zookeeperClient: ZookeeperClient, val controller:ZkController, val socketServer: SimpleSocketServer,
             kip500Mode:Boolean = false) extends Logging {

  val executor = new ScheduledThreadPoolExecutor(1);


  def sendHearbeat() = {
    val updateMetadataRequest = BrokerHeartbeat(config.brokerId, InetAddressAndPort.create(config.hostName, config.port))
    val request = RequestOrResponse(com.dist.simplekafka.kip500.election.RequestKeys.BrokerHeartbeat.asInstanceOf[Short], JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet())
    info(s"Sending heartbeat from broker ${config.brokerId}")
    val response = socketServer.sendReceiveTcp(request, config.kip500ControllerAddress)
    info(s"Received heartbeat response in broker ${config.brokerId} ${response.messageBodyJson}")
  }


  var scheduledTasks:ScheduledFuture[_] = _

  var fetchOffset:Long = 0
  def fetchMetadata() = {
    val updateMetadataRequest = FetchRequest(fetchOffset)
    val request = RequestOrResponse(com.dist.simplekafka.kip500.election.RequestKeys.Fetch.asInstanceOf[Short], JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet())
    info(s"Sending fetch request from broker ${config.brokerId}")
    val response = socketServer.sendReceiveTcp(request, config.kip500ControllerAddress)
    val fetchResponse = JsonSerDes.deserialize(response.messageBodyJson, classOf[FetchResponse])
    info(s"Received fetch response in broker ${config.brokerId} ${fetchResponse}")
    fetchResponse.walEntries.foreach(walEntry => {
      applyEntry(walEntry)
    })
    fetchOffset = fetchResponse.walEntries.last.entryId
  }



  def applyEntry(entry: WalEntry) = {
    if (entry.entryType == EntryType.data) {
      val command = Record.deserialize(new ByteArrayInputStream(entry.data))
      command match {
        case brokerHeartbeat: BrokerHeartbeat => {
          val brokerId = brokerHeartbeat.brokerId
          info(s"Registering Active Broker with id ${brokerId}")
          val broker = Broker(brokerId, brokerHeartbeat.address.address.getHostAddress, brokerHeartbeat.address.port)
          controller.liveBrokers += broker
          socketServer.kafkaApis.aliveBrokers += broker
        }
        case topicRecord: TopicRecord => {
          println(topicRecord)
        }
        case partitionRecord: PartitionRecord => {
          val leaderBroker = controller.getLiveBroker(partitionRecord.leader).get
          val replicaBrokers = partitionRecord.replicas.map(id => controller.getLiveBroker(id).get)
          val topicPartition = TopicAndPartition(partitionRecord.topicId, partitionRecord.partitionId)
          val leaderAndReplicas = LeaderAndReplicas(topicPartition,
                                           PartitionInfo(leaderBroker, replicaBrokers))
          socketServer.kafkaApis.handleLeaderAndReplicas(List(leaderAndReplicas))
          info("Updating leader cache in " + config.brokerId)
          socketServer.kafkaApis.leaderCache.put(topicPartition, PartitionInfo(leaderBroker, replicaBrokers))
        }
        case fenceBroker:FenceBroker => {
          println(fenceBroker)
        }
      }
    }
  }

  def startup() = {
    socketServer.startup()
    if (kip500Mode) {
      socketServer.kafkaApis.aliveBrokers = ListBuffer[Broker]()
      sendHearbeat()
      scheduledTasks = executor.scheduleWithFixedDelay(new FetchMetadataTask(fetchMetadata), 1000, 1000, TimeUnit.MILLISECONDS)
      info(s"Starting hearbeat task ${1000} ms")

    } else {
      zookeeperClient.registerSelf()
      controller.startup()
    }

    info(s"Server ${config.brokerId} started with log dir ${config.logDirs}")
  }

  class FetchMetadataTask(action:()⇒Unit) extends Runnable {
    override def run(): Unit = {
      action()
    }
  }

  def shutdown()= {
    zookeeperClient.shutdown()
    socketServer.shutdown()
  }

  val correlationId = new AtomicInteger(0)

}
