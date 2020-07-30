package com.dist.consensus

import java.io.{ByteArrayInputStream, File}
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicReference

import com.dist.consensus.ServerState.ServerState
import com.dist.consensus.election.{RequestKeys, Vote, VoteResponse}
import com.dist.consensus.network.{Config, InetAddressAndPort, JsonSerDes, Peer, PeerProxy, RequestOrResponse, TcpListener}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object ServerState extends Enumeration {
  type ServerState = Value
  val LOOKING, FOLLOWING, LEADING = Value
}

class Server(config: Config) extends Thread with Logging {
  def shutdown(): Any = {
    if (leader != null) {
      leader.followerProxies.foreach(p => p.stop())
    }

    listener.shudown()
  }

  var commitIndex = 0L

  var leader: Leader = _

  val kv = new KVStore(config.walDir)
  val electionTimeoutChecker = new HeartBeatScheduler(heartBeatCheck)

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
  private val client = new NetworkClient()


  def handleAppendEntries(appendEntryRequest: AppendEntriesRequest) = {
    heartBeatReceived = true

    val lastLogEntry = this.kv.wal.lastLogEntryId
    if (appendEntryRequest.walEntry == null) { //this is heartbeat
      updateCommitIndex(appendEntryRequest)
      AppendEntriesResponse(lastLogEntry, true)

    } else if (lastLogEntry >= appendEntryRequest.walEntry.entryId) {
      AppendEntriesResponse(lastLogEntry, false)

    } else {
      this.kv.wal.writeEntry(appendEntryRequest.walEntry)
      updateCommitIndex(appendEntryRequest)
    }
  }

  private def updateCommitIndex(appendEntryRequest: AppendEntriesRequest) = {
    if (this.commitIndex < appendEntryRequest.commitIndex) {
      updateCommitIndexAndApplyEntries(appendEntryRequest.commitIndex)
    }
    AppendEntriesResponse(this.kv.wal.lastLogEntryId, true)
  }

  def requestHandler(request: RequestOrResponse) = {
    if (request.requestId == RequestKeys.RequestVoteKey) {
      val vote = VoteResponse(currentVote.get().id, currentVote.get().lastLogIndex)
      info(s"Responding vote response from ${config.serverId} be ${currentVote}")
      RequestOrResponse(RequestKeys.RequestVoteKey, JsonSerDes.serialize(vote), request.correlationId)
    } else if (request.requestId == RequestKeys.AppendEntriesKey) {

      val appendEntries = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[AppendEntriesRequest])
      val appendEntriesResponse = handleAppendEntries(appendEntries)
      info(s"Responding AppendEntriesResponse from ${config.serverId} be ${appendEntriesResponse}")
      RequestOrResponse(RequestKeys.AppendEntriesKey, JsonSerDes.serialize(appendEntriesResponse), request.correlationId)

    }
    else throw new RuntimeException("UnknownRequest")
  }

  def applyEntries(entries: ListBuffer[WalEntry]) = {
    entries.foreach(entry ⇒ {
      val value = kv.applyEntry(entry)
      val promise: Promise[Any] = pendingRequests.get(entry.entryId)
      if (promise != null) {
        promise.success(value)
      }
    })
  }

  def updateCommitIndexAndApplyEntries(index: Long) = {
    val previousCommitIndex = commitIndex
    commitIndex = index
    kv.wal.highWaterMark = commitIndex
    if (commitIndex <= kv.wal.lastLogEntryId) {
      info(s"Applying wal entries in ${config.serverId} from ${previousCommitIndex} to ${commitIndex}")
      val entries = kv.wal.entries(previousCommitIndex, commitIndex)
      applyEntries(entries)
    }

  }

  val listener = new TcpListener(config.serverAddress, requestHandler)

  def startListening() = {
    listener.start()
  }

  val currentVote = new AtomicReference(Vote(config.serverId, kv.wal.lastLogEntryId))

  @volatile var state: ServerState.Value = ServerState.LOOKING

  override def run(): Unit = {
    transitionTo(ServerState.LOOKING)
  }

  def setPeerState(serverState: ServerState.Value) = this.state = serverState

  val pendingRequests = new ConcurrentHashMap[Long, Promise[Any]]()

  def registerClient(clientId:String): Unit = {
    propose(RegisterClientCommand(clientId))
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  def put(key: String, value: String) = {
    propose(SetValueCommand(key, value))
  }


  private def propose(command:Command) = {
    if (leader == null) throw new RuntimeException("Can not propose to non leader")

    //propose is synchronous as of now so value will be applied
    val future = leader.propose(command)
    future
  }

  def addPendingRequest(logIndex:Long, promise:Promise[Any]) = {
    pendingRequests.put(logIndex, promise)
  }

  def get(key: String) = {
    kv.get(key)
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
  }
}


case class AppendEntriesRequest(walEntry:WalEntry, commitIndex: Long)

case class AppendEntriesResponse(xid: Long, success: Boolean)

class Leader(config: Config, client: NetworkClient, val self: Server) extends Logging {
  val followerProxies = config.getPeers().map(p ⇒ PeerProxy(p, 0, sendHeartBeat))

  def startLeading() = {
    followerProxies.foreach(_.start())
  }

  def sendHeartBeat(peerProxy: PeerProxy) = {
    info(s"Sending heartbeat from ${config.serverId} to ${peerProxy.peerInfo.id}")
    val appendEntries = JsonSerDes.serialize(AppendEntriesRequest(null, self.kv.wal.highWaterMark))
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

  var lastEntryId: Long = self.kv.wal.lastLogEntryId

  def propose(setValueCommand: Command) = {
    val resultPromise = Promise[Any]()
    val data = setValueCommand.serialize()
    val entryId = appendToLocalLog(data)
    self.addPendingRequest(entryId, resultPromise)
    broadCastAppendEntries(self.kv.wal.entries(entryId - 1, entryId).last)
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
    lastEntryId = self.kv.wal.writeEntry(data)
    lastEntryId
  }
}
