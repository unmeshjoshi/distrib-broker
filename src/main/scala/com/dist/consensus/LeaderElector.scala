package com.dist.consensus

import com.dist.consensus.election.{ElectionResult, Elector, RequestKeys, Vote, VoteRequest, VoteResponse}
import com.dist.consensus.network.{Config, InetAddressAndPort, JsonSerDes, Peer, RequestOrResponse}

import scala.util.control.Breaks.{break, breakable}
import scala.jdk.CollectionConverters._

class LeaderElector(config:Config, self:Server, peers:List[Peer]) extends Logging {
  val client = new NetworkClient()
  def lookForLeader(): Unit = {
    self.currentVote.set(Vote(config.serverId, self.kv.wal.lastLogEntryId))
    breakable {

      while (true) {

        val votes = getVotesFromPeers
        val electionResult = new Elector(config.clusterSize()).elect(votes.asScala.toMap)

        if (electionResult.isElected()) {
          setLeaderOrFollowerState(electionResult)
          break

        } else {

          setCurrentVoteToWouldBeLeaderVote(electionResult)
        }

        info(s"${config.serverId} Waiting for leader to be selected: " + votes)
      }
    }
  }

  private def getVotesFromPeers() = {
    val votes = new java.util.HashMap[InetAddressAndPort, Vote]
    votes.put(config.serverAddress, selfVote())
    peers.foreach(peer ⇒ {
      try {
      val request = VoteRequest(config.serverId, self.kv.wal.lastLogEntryId)
      val voteRequest = RequestOrResponse(RequestKeys.RequestVoteKey, JsonSerDes.serialize(request), 0)
      val response = client.sendReceive(voteRequest, peer.address)
      val maybeVote = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[VoteResponse])
      votes.put(peer.address, Vote(maybeVote.serverId, maybeVote.lastXid))
      } catch {
        case e:Exception => logger.error(e)
      }
    })
    votes
  }


  private def selfVote() = {
    self.currentVote.get()
  }

  private def setLeaderOrFollowerState(electionResult: ElectionResult) = {
    //set state as leader
    self.currentVote.set(electionResult.winningVote)
    if (electionResult.winningVote.id == config.serverId) {
      info(s"Setting ${electionResult.winningVote.id} to be leader")
      self.transitionTo(ServerState.LEADING)
    } else {
      info(s"Setting ${config.serverId} to be follower of ${electionResult.winningVote.id}")
      self.transitionTo(ServerState.FOLLOWING)
    }
  }


  private def setCurrentVoteToWouldBeLeaderVote(electionResult: ElectionResult) = {
    info(s"Setting current vote in ${config.serverId} to ${electionResult.vote}")
    self.currentVote.set(electionResult.vote)
  }
}
