package com.dist.simplekafka.api

import com.dist.simplekafka.common.JsonSerDes

object Request {
  val FollowerReplicaFetcherId: Int = 0
  val OrdinaryConsumerId: Int = -1
  val DebuggingConsumerId: Int = -2
}


object RequestKeys {
  val BrokerHeartbeat: Short = 9

  val ProduceKey: Short = 0
  val FetchKey: Short = 1
  val OffsetsKey: Short = 2
  val GetMetadataKey: Short = 3
  val LeaderAndIsrKey: Short = 4
  val StopReplicaKey: Short = 5
  val UpdateMetadataKey: Short = 6
  val ControlledShutdownKey: Short = 7
  val FindCoordinatorKey:Short = 8
  val FindCoordinator:Short = 9
  val FindGroupCoordinator:Short = 10
  val OffsetCommitRequest:Short = 11

}

case class RequestOrResponse(val requestId: Short, val messageBodyJson: String, val correlationId: Int) {
  def serialize(): String = {
    JsonSerDes.serialize(this)
  }
}