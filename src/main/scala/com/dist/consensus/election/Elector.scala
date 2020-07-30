package com.dist.consensus.election

import com.dist.consensus.Logging
import com.dist.consensus.network.InetAddressAndPort

class Elector(noOfServers: Int) extends Logging {
  def elect(votes: Map[InetAddressAndPort, Vote]): ElectionResult = {
    var result = ElectionResult(Vote(Long.MinValue, Long.MinValue), 0, Vote(Long.MinValue, Long.MinValue), 0, noOfServers)
    val voteCounts = votes.values.groupBy(identity).mapValues(_.size)
    val max: (Vote, Int) = voteCounts.maxBy(tuple ⇒ tuple._2)

    votes.values.foreach(v ⇒ {
      if (v.lastLogIndex > result.vote.lastLogIndex || (v.lastLogIndex == result.vote.lastLogIndex && v.id > result.vote.id)) {
        result = result.copy(vote = v, count = 1)
      }
    })
    result.copy(winningVote = max._1, winningCount = max._2)
  }
}

case class ElectionResult(vote: Vote, count: Int, winningVote: Vote, winningCount: Int, noOfServers: Int) {
  def isElected() = {
    winningCount > (noOfServers / 2)
  }
}
