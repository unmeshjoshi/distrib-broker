package com.dist.simplekafka.kip500

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import com.dist.simplekafka.kip500.ServerState.ServerState
import com.dist.simplekafka.kip500.election.{RequestKeys, Vote, VoteResponse}
import com.dist.simplekafka.kip500.network.{Config, JsonSerDes, RequestOrResponse}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}


object ServerState extends Enumeration {
  type ServerState = Value
  val LOOKING, FOLLOWING, LEADING = Value
}

object Response {
  val None = new Response()
}

class Response(){

}

trait Consensus {
  def getState():ServerState

  def propose(command: Record): Future[Response]

  def readEntries(startOffset: Long): List[WalEntry]

  def handleRaftRequest(request: RequestOrResponse): Future[RequestOrResponse]

  def start()

  def shutdown()

}

trait StateMachine {
  def applyEntry(entry: WalEntry):Response;

  def applyEntries(walEntries: List[WalEntry]):List[Response]

  def onBecomingLeader

  def onBecomingFollower
}

class ConsensusImpl(config: Config, stateMachine: StateMachine) extends Consensus with Logging {
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
      val promise: Promise[Response] = pendingRequests.get(entry.entryId)
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

  val pendingRequests = new ConcurrentHashMap[Long, Promise[Response]]()


  def propose(command: Record) = {
    if (leader == null) throw new RuntimeException("Can not propose to non leader")

    //propose is synchronous as of now so value will be applied
    val future = leader.propose(command)
    future
  }

  def addPendingRequest(logIndex: Long, promise: Promise[Response]) = {
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