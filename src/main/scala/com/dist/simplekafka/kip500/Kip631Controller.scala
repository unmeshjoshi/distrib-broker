package com.dist.simplekafka.kip500

import com.dist.simplekafka.kip500.election.RequestKeys
import com.dist.simplekafka.kip500.network._
import com.dist.simplekafka.util.AdminUtils

import scala.concurrent.{Future, Promise}

class Kip631Controller(val config: Config) extends Thread with StateMachine with Logging {
  val consensus: Consensus = new ConsensusImpl(config, this)

  val controllerState = new ControllerState()

  def applyEntries(walEntries: List[WalEntry]): List[Response] = {
    controllerState.applyEntries(walEntries)
  }

  def onBecomingLeader = {
    controllerState.leaseTracker.stop()
    controllerState.leaseTracker = new LeaderLeaseTracker(config, controllerState.activeBrokers, new SystemClock(), consensus)
    controllerState.leaseTracker.start()
  }

  def onBecomingFollower = {
    controllerState.leaseTracker.stop()
    controllerState.leaseTracker = new FollowerLeaseTracker(controllerState.activeBrokers)
    controllerState.leaseTracker.start()
  }

  def createTopic(topicName: String, noOfPartitions: Int, replicationFactor: Int) = {
    val keys = controllerState.getActiveBrokerIds()
    val partitionRecords = assignReplicasToBrokers(topicName, noOfPartitions, replicationFactor, keys)
    val topicRecordFuture = consensus.propose(TopicRecord(topicName))
    val partitionRecordFutures = partitionRecords.map(partitionRecord => consensus.propose(partitionRecord))
    import scala.concurrent.ExecutionContext.Implicits.global
    Future.sequence(partitionRecordFutures + topicRecordFuture)
  }

  private def assignReplicasToBrokers(topicName: String, noOfPartitions: Int, replicationFactor: Int, keys: Iterator[Int]) = {
    val partitionAssignments = AdminUtils.assignReplicasToBrokers(keys.toList, noOfPartitions, replicationFactor)
    val partitionIds = partitionAssignments.toMap.keySet
    val partitionRecords = partitionIds.map(partitionId => {
      val replicas = partitionAssignments(partitionId)
      val leader = replicas.head
      PartitionRecord(partitionId, topicName, replicas.toList, leader)
    })
    partitionRecords
  }

  def brokerHeartbeat(brokerHeartbeat: BrokerHeartbeat) = {
    consensus.propose(brokerHeartbeat)
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  class ControllerAPI(controller:Kip631Controller) {
    def handleRequest(request:RequestOrResponse):Future[RequestOrResponse] = {
      if (request.requestId == RequestKeys.Fetch) {
        val fetchRequest = deserialize(request, classOf[FetchRequest])
        val logEntries = consensus.readEntries(fetchRequest.fromOffset)
        info("Responding with log entries " + logEntries)
        Future.successful(RequestOrResponse(RequestKeys.Fetch, serialize(FetchResponse(logEntries.toList)), request.correlationId))

      } else if (request.requestId == RequestKeys.BrokerHeartbeat) {
        val brokerHeartbeat = deserialize(request, classOf[BrokerHeartbeat])
        val future: Future[Response] = controller.brokerHeartbeat(brokerHeartbeat)
        future.map((response) => {
          val brokerRegistration = response.asInstanceOf[BrokerRegistrationResponse]
          val heartbeatResponse = serialize(BrokerHeartbeatResponse(brokerRegistration.errorCode, brokerRegistration.brokerEpoch))
          RequestOrResponse(RequestKeys.BrokerHeartbeat.asInstanceOf[Short], heartbeatResponse, request.correlationId)
        })

      } else if (request.requestId == RequestKeys.CreateTopic) {
        val createTopicRequest = deserialize(request, classOf[CreateTopicRequest])
        val future = controller.createTopic(createTopicRequest.topicName, createTopicRequest.noOfPartitions, createTopicRequest.replicationFactor)
        future.map(f => {
          RequestOrResponse(RequestKeys.CreateTopic.asInstanceOf[Short], JsonSerDes.serialize(CreateTopicResponse(createTopicRequest.topicName)), 0)
        })
      } else throw new IllegalArgumentException(s"Invalid requestId ${request.requestId}")
    }

    private def serialize(response:Any) = {
      JsonSerDes.serialize(response)
    }

    private def deserialize[T](request: RequestOrResponse, clazz: Class[T]):T = {
      JsonSerDes.deserialize(request.messageBodyJson.getBytes(), clazz)
    }
  }

  val controllerAPI:ControllerAPI = new ControllerAPI(this)
  def requestHandler(request: RequestOrResponse): Future[RequestOrResponse] = {
    if (isConsensusRequest(request)) {
      return consensus.handleRaftRequest(request)
    }
    controllerAPI.handleRequest(request)
  }


  val listener = new TcpListener(config.serverAddress, requestHandler)

  def startListening() = {
    listener.start()
  }

  override def run(): Unit = {
    consensus.start()
  }

  def shutdown() = {
    listener.shudown()
    consensus.shutdown();
  }


  private def isConsensusRequest(request: RequestOrResponse) = {
    request.requestId == RequestKeys.RequestVoteKey || request.requestId == RequestKeys.AppendEntriesKey
  }

  override def applyEntry(entry: WalEntry) = {
    controllerState.applyEntry(entry)
  }
}

case class FetchRequest(fromOffset: Long = 0)

case class FetchResponse(walEntries: List[WalEntry])

case class BrokerHeartbeatResponse(errorCode:Int, LeaseEndTimeMs: Long)

