package com.dist.simplekafka

import com.dist.simplekafka.api.RequestOrResponse
import com.dist.simplekafka.common.TopicAndPartition

import scala.collection.mutable

object TransactionState {
  val Empty  = 0
  val OnGoing  = 1
  val PrepareCommit  = 2
  val PrepareAbort  = 3
  val CompleteCommit  = 4
  val CompleteAbort  = 5

}

case class TransactionMetadata(val transactionalId: String,
                               var producerId: Long,
                               var txnTimeoutMs: Int,
                               var state: Int,
                               var topicPartitions: mutable.Set[TopicAndPartition],
                               @volatile var txnStartTimestamp: Long = -1,
                               @volatile var txnLastUpdateTimestamp: Long) {
  def prepareAddPartitions(addedTopicPartitions: List[TopicAndPartition], timestamp:Long) = {
    topicPartitions = topicPartitions ++ addedTopicPartitions
    txnLastUpdateTimestamp = timestamp
  }

}
