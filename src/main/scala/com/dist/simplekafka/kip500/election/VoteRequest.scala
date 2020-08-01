package com.dist.simplekafka.kip500.election

object RequestKeys {
  val CreateTopic = 4

  val RequestVoteKey: Short = 0
  val AppendEntriesKey: Short = 1
  val Fetch:Short = 2
  val BrokerHeartbeat = 3
}

case class VoteRequest(serverId:Long, lastXid:Long)

case class VoteResponse(serverId:Long, lastXid:Long)