package com.dist.simplekafka.kip500

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import com.dist.simplekafka.kip500.ServerState.ServerState
import com.dist.simplekafka.kip500.election.{RequestKeys, Vote, VoteResponse}
import com.dist.simplekafka.kip500.network._
import com.dist.simplekafka.util.AdminUtils

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}

object ServerState extends Enumeration {
  type ServerState = Value
  val LOOKING, FOLLOWING, LEADING = Value
}

trait Consensus {
  def getState():ServerState

  def propose(command: Record): Future[Any]

  def readEntries(startOffset: Long): List[WalEntry]

  def handleRaftRequest(request: RequestOrResponse): Future[RequestOrResponse]

  def start()

  def shutdown()

}

trait StateMachine {
  def applyEntry(entry: WalEntry)

  def applyEntries(walEntries: List[WalEntry])

  def onBecomingLeader

  def onBecomingFollower
}

class RaftConsensus(config: Config, stateMachine: StateMachine) extends Consensus with Logging {
  var commitIndex = 0L
  val wal = WriteAheadLog.create(config.walDir)

  val electionTimeoutChecker = new HeartBeatScheduler(heartBeatCheck)


  override def readEntries(startOffset: Long): List[WalEntry] = {
    wal.entries(startOffset, wal.highWaterMark).toList
  }

  def handleHeartBeatTimeout(): Unit = {
    info(s"Heartbeat timeout starting election in ${config.serverId}")
    transitionTo(ServerState.LOOKING)
  }

  var heartBeatReceived = false

  def heartBeatCheck(): Unit = {
    info(s"Checking if heartbeat received in ${state} ${config.serverId}")
    if (!heartBeatReceived) {
      handleHeartBeatTimeout()
    } else {
      heartBeatReceived = false //reset
    }
  }


  @volatile var running = true

  def handleAppendEntries(appendEntryRequest: AppendEntriesRequest) = {
    heartBeatReceived = true
    val lastLogEntry = this.wal.lastLogEntryId
    if (appendEntryRequest.walEntry == null) { //this is heartbeat
      updateCommitIndex(appendEntryRequest)
      AppendEntriesResponse(lastLogEntry, true)

    } else if (lastLogEntry >= appendEntryRequest.walEntry.entryId) {
      AppendEntriesResponse(lastLogEntry, false)

    } else {
      info(s"Writing walEntry ${appendEntryRequest.walEntry} in ${this.config.serverId} ")
      this.wal.writeEntry(appendEntryRequest.walEntry)
      updateCommitIndex(appendEntryRequest)
    }
  }

  private def updateCommitIndex(appendEntryRequest: AppendEntriesRequest) = {
    if (this.commitIndex < appendEntryRequest.commitIndex) {
      updateCommitIndexAndApplyEntries(appendEntryRequest.commitIndex)
    }
    AppendEntriesResponse(this.wal.lastLogEntryId, true)
  }

  override def handleRaftRequest(request: RequestOrResponse): Future[RequestOrResponse] = {
    if (request.requestId == RequestKeys.RequestVoteKey) {
      val vote = VoteResponse(currentVote.get().id, currentVote.get().lastLogIndex)
      info(s"Responding vote response from ${config.serverId} be ${currentVote}")
      Future.successful(RequestOrResponse(RequestKeys.RequestVoteKey, JsonSerDes.serialize(vote), request.correlationId))
    } else if (request.requestId == RequestKeys.AppendEntriesKey) {

      val appendEntries = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[AppendEntriesRequest])
      val appendEntriesResponse = handleAppendEntries(appendEntries)
      info(s"Responding AppendEntriesResponse from ${config.serverId} be ${appendEntriesResponse}")
      Future.successful(RequestOrResponse(RequestKeys.AppendEntriesKey, JsonSerDes.serialize(appendEntriesResponse), request.correlationId))
    } else throw new IllegalArgumentException(s"Invalid request ${request.requestId}")
  }

  def applyEntries(entries: ListBuffer[WalEntry]) = {
    entries.foreach(entry ⇒ {
      val value = stateMachine.applyEntry(entry)
      val promise: Promise[Any] = pendingRequests.get(entry.entryId)
      if (promise != null) {
        promise.success(value)
      }
    })
  }

  def updateCommitIndexAndApplyEntries(index: Long) = {
    val previousCommitIndex = commitIndex
    commitIndex = index
    wal.highWaterMark = commitIndex
    if (commitIndex <= wal.lastLogEntryId && commitIndex > 0) {
      info(s"Applying wal entries in ${config.serverId} from ${previousCommitIndex} to ${commitIndex}")
      val entries = wal.entries(previousCommitIndex, commitIndex)
      applyEntries(entries)
    }

  }

  val currentVote = new AtomicReference(Vote(config.serverId, wal.lastLogEntryId))

  @volatile var state: ServerState.Value = ServerState.LOOKING

  def setPeerState(serverState: ServerState.Value) = this.state = serverState

  val pendingRequests = new ConcurrentHashMap[Long, Promise[Any]]()


  def propose(command: Record) = {
    if (leader == null) throw new RuntimeException("Can not propose to non leader")

    //propose is synchronous as of now so value will be applied
    val future = leader.propose(command)
    future
  }

  def addPendingRequest(logIndex: Long, promise: Promise[Any]) = {
    pendingRequests.put(logIndex, promise)
  }


  def transitionTo(state: ServerState) = {
    this.state = state
    electionTimeoutChecker.cancel()

    if (this.state == ServerState.LOOKING) {
      try {
        val electionResult = new LeaderElector(config, this, config.getPeers()).lookForLeader()
      } catch {
        case e: Exception ⇒ {
          e.printStackTrace()
          this.state = ServerState.LOOKING
        }
      }
    } else if (this.state == ServerState.LEADING) {
      this.leader = new Leader(config, new NetworkClient(), this)
      this.leader.startLeading()


    } else if (this.state == ServerState.FOLLOWING) {
      electionTimeoutChecker.startWithRandomInterval()
    }

    if (this.state == ServerState.LEADING)
      stateMachine.onBecomingLeader
    else
      stateMachine.onBecomingFollower
  }


  var leader: Leader = _

  override def shutdown() = {
    if (leader != null) {
      leader.followerProxies.foreach(p => p.stop())
    }
  }

  override def start(): Unit = {
    transitionTo(ServerState.LOOKING)
  }

  override def getState(): ServerState = state
}

class Kip500Controller(val config: Config) extends Thread with StateMachine with Logging {
  val consensus: Consensus = new RaftConsensus(config, this)

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

  val controllerState = new ControllerState()

  def applyEntry(entry: WalEntry): Unit = {
    controllerState.applyEntry(entry)
  }

  def applyEntries(walEntries: List[WalEntry]): Unit = {
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


  def brokerHeartbeat(brokerHeartbeat: BrokerHeartbeat) = {
    val future = consensus.propose(brokerHeartbeat)
    future
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


  import scala.concurrent.ExecutionContext.Implicits.global


  def requestHandler(request: RequestOrResponse): Future[RequestOrResponse] = {
    if (request.requestId == RequestKeys.RequestVoteKey || request.requestId == RequestKeys.AppendEntriesKey) {
      return consensus.handleRaftRequest(request)
    }

    if (request.requestId == RequestKeys.Fetch) {
      val fetchRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[FetchRequest])
      val logEntries = consensus.readEntries(fetchRequest.fromOffset)
      info("Responding with log entries " + logEntries)
      Future.successful(RequestOrResponse(RequestKeys.Fetch, JsonSerDes.serialize(FetchResponse(logEntries.toList)), request.correlationId))

    } else if (request.requestId == RequestKeys.BrokerHeartbeat) {
      val brokerHeartbeat = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[BrokerHeartbeat])
      val future = this.brokerHeartbeat(brokerHeartbeat)
      future.map(f => {
        val response = BrokerHeartbeatResponse(controllerState.activeBrokers.get(brokerHeartbeat.brokerId).expirationTime)
        RequestOrResponse(RequestKeys.BrokerHeartbeat.asInstanceOf[Short], JsonSerDes.serialize(response), 0)
      })
    } else if (request.requestId == RequestKeys.CreateTopic) {
      val createTopicRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[CreateTopicRequest])
      val future = this.createTopic(createTopicRequest.topicName, createTopicRequest.noOfPartitions, createTopicRequest.replicationFactor)
      future.map(f => {
        RequestOrResponse(RequestKeys.CreateTopic.asInstanceOf[Short], JsonSerDes.serialize(CreateTopicResponse(createTopicRequest.topicName)), 0)
      })
    } else throw new IllegalArgumentException(s"Invalid requestId ${request.requestId}")

  }
}

case class FetchRequest(fromOffset: Long = 0)

case class FetchResponse(walEntries: List[WalEntry])

case class AppendEntriesRequest(walEntry: WalEntry, commitIndex: Long)

case class AppendEntriesResponse(xid: Long, success: Boolean)

case class BrokerHeartbeatResponse(LeaseEndTimeMs: Long)

class Leader(config: Config, client: NetworkClient, val self: RaftConsensus) extends Logging {
  val followerProxies = config.getPeers().map(p ⇒ PeerProxy(p, 0, sendHeartBeat))

  def startLeading() = {
    followerProxies.foreach(_.start())
  }

  def sendHeartBeat(peerProxy: PeerProxy) = {
    info(s"Sending heartbeat from ${config.serverId} to ${peerProxy.peerInfo.id}")
    val appendEntries = JsonSerDes.serialize(AppendEntriesRequest(null, self.wal.highWaterMark))
    val request = RequestOrResponse(RequestKeys.AppendEntriesKey, appendEntries, 0)
    //sendHeartBeat
    val response = client.sendReceive(request, peerProxy.peerInfo.address)
    //TODO: Handle response
    val appendOnlyResponse: AppendEntriesResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[AppendEntriesResponse])
    if (appendOnlyResponse.success) {
      peerProxy.matchIndex = appendOnlyResponse.xid
    } else {
      // TODO: handle term and failures
    }
  }

  def stopLeading() = followerProxies.foreach(_.stop())

  var lastEntryId: Long = self.wal.lastLogEntryId

  def propose(setValueCommand: Record) = {
    val resultPromise = Promise[Any]()
    val data = setValueCommand.serialize()
    val entryId = appendToLocalLog(data)
    self.addPendingRequest(entryId, resultPromise)
    broadCastAppendEntries(self.wal.entries(entryId - 1, entryId).last)
    resultPromise.future
  }

  private def findMaxIndexWithQuorum = {
    val matchIndexes = followerProxies.map(p ⇒ p.matchIndex)
    val sorted: Seq[Long] = matchIndexes.sorted
    val matchIndexAtQuorum = sorted((config.peerConfig.size - 1) / 2)
    matchIndexAtQuorum
  }

  private def broadCastAppendEntries(walEntry: WalEntry) = {
    val request = appendEntriesRequestFor(walEntry)

    //TODO: Happens synchronously for demo. Has to be async with each peer having its own thread
    followerProxies.map(peer ⇒ {
      val response = client.sendReceive(request, peer.peerInfo.address)
      val appendEntriesResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[AppendEntriesResponse])

      peer.matchIndex = appendEntriesResponse.xid

      val matchIndexAtQuorum = findMaxIndexWithQuorum


      info(s"Peer match indexes are at ${config.peerConfig}")
      info(s"CommitIndex from quorum is ${matchIndexAtQuorum}")

      if (self.commitIndex < matchIndexAtQuorum) {
        self.updateCommitIndexAndApplyEntries(matchIndexAtQuorum)
      }
    })
  }

  private def appendEntriesRequestFor(walEntry: WalEntry) = {
    val appendEntries = JsonSerDes.serialize(AppendEntriesRequest(walEntry, self.commitIndex))
    val request = RequestOrResponse(RequestKeys.AppendEntriesKey, appendEntries, 0)
    request
  }

  private def appendToLocalLog(data: Array[Byte]) = {
    lastEntryId = self.wal.writeEntry(data)
    lastEntryId
  }

}

