package com.dist.simplekafka.kip500.election

object RequestKeys {
  val RequestVoteKey: Short = 0
  val AppendEntriesKey: Short = 1
}

case class VoteRequest(serverId:Long, lastXid:Long)

case class VoteResponse(serverId:Long, lastXid:Long)