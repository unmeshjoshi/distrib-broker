package com.dist.simplekafka

import com.dist.simplekafka.common.TopicAndPartition

import scala.collection.{immutable, mutable}
import java.util

case class TxnMetadataCacheEntry(metadataPerTransactionalId: util.Map[String, TransactionMetadata]) {
  override def toString: String = {
    val epoch = -1 //TODO
    s"TxnMetadataCacheEntry(coordinatorEpoch=$epoch, numTransactionalEntries=${metadataPerTransactionalId.size})"
  }
}

object TransactionState {
  val AllStates = Set(
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
    PrepareEpochFence
  )

  def fromName(name: String): Option[TransactionState] = {
    AllStates.find(_.name == name)
  }

  def fromId(id: Byte): TransactionState = {
    id match {
      case 0 => Empty
      case 1 => Ongoing
      case 2 => PrepareCommit
      case 3 => PrepareAbort
      case 4 => CompleteCommit
      case 5 => CompleteAbort
      case 6 => Dead
      case 7 => PrepareEpochFence
      case _ => throw new IllegalStateException(s"Unknown transaction state id $id from the transaction status message")
    }
  }
}

sealed trait TransactionState {
  def id: Byte

  /**
   * Get the name of this state. This is exposed through the `DescribeTransactions` API.
   */
  def name: String
}

/**
 * Transaction has not existed yet
 *
 * transition: received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private case object Empty extends TransactionState {
  val id: Byte = 0
  val name: String = "Empty"
}

/**
 * Transaction has started and ongoing
 *
 * transition: received EndTxnRequest with commit => PrepareCommit
 *             received EndTxnRequest with abort => PrepareAbort
 *             received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private case object Ongoing extends TransactionState {
  val id: Byte = 1
  val name: String = "Ongoing"
}

/**
 * Group is preparing to commit
 *
 * transition: received acks from all partitions => CompleteCommit
 */
private case object PrepareCommit extends TransactionState {
  val id: Byte = 2
  val name: String = "PrepareCommit"
}

/**
 * Group is preparing to abort
 *
 * transition: received acks from all partitions => CompleteAbort
 */
private case object PrepareAbort extends TransactionState {
  val id: Byte = 3
  val name: String = "PrepareAbort"
}

/**
 * Group has completed commit
 *
 * Will soon be removed from the ongoing transaction cache
 */
private case object CompleteCommit extends TransactionState {
  val id: Byte = 4
  val name: String = "CompleteCommit"
}

/**
 * Group has completed abort
 *
 * Will soon be removed from the ongoing transaction cache
 */
private case object CompleteAbort extends TransactionState {
  val id: Byte = 5
  val name: String = "CompleteAbort"
}

/**
 * TransactionalId has expired and is about to be removed from the transaction cache
 */
private case object Dead extends TransactionState {
  val id: Byte = 6
  val name: String = "Dead"
}

/**
 * We are in the middle of bumping the epoch and fencing out older producers.
 */

private case object PrepareEpochFence extends TransactionState {
  val id: Byte = 7
  val name: String = "PrepareEpochFence"
}


// this is a immutable object representing the target transition of the transaction metadata
private case class TxnTransitMetadata(producerId: Long,
                                                   lastProducerId: Long,
                                                   producerEpoch: Short,
                                                   lastProducerEpoch: Short,
                                                   txnTimeoutMs: Int,
                                                   txnState: TransactionState,
                                                   topicPartitions: immutable.Set[TopicAndPartition],
                                                   txnStartTimestamp: Long,
                                                   txnLastUpdateTimestamp: Long) {
  override def toString: String = {
    "TxnTransitMetadata(" +
      s"producerId=$producerId, " +
      s"lastProducerId=$lastProducerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"lastProducerEpoch=$lastProducerEpoch, " +
      s"txnTimeoutMs=$txnTimeoutMs, " +
      s"txnState=$txnState, " +
      s"topicPartitions=$topicPartitions, " +
      s"txnStartTimestamp=$txnStartTimestamp, " +
      s"txnLastUpdateTimestamp=$txnLastUpdateTimestamp)"
  }
}


object TransactionMetadata {
  def isValidTransition(oldState: TransactionState, newState: TransactionState): Boolean =
    TransactionMetadata.validPreviousStates(newState).contains(oldState)

  private val validPreviousStates: Map[TransactionState, Set[TransactionState]] =
    Map(Empty -> Set(Empty, CompleteCommit, CompleteAbort),
      Ongoing -> Set(Ongoing, Empty, CompleteCommit, CompleteAbort),
      PrepareCommit -> Set(Ongoing),
      PrepareAbort -> Set(Ongoing, PrepareEpochFence),
      CompleteCommit -> Set(PrepareCommit),
      CompleteAbort -> Set(PrepareAbort),
      Dead -> Set(Empty, CompleteAbort, CompleteCommit),
      PrepareEpochFence -> Set(Ongoing)
    )

  def isEpochExhausted(producerEpoch: Short): Boolean = producerEpoch >= Short.MaxValue - 1
}

case class TransactionMetadata(val transactionalId: String,
                               val producerId: Long,
                               val producerEpoch:Long,
                               val txnTimeoutMs: Int,
                               val state: TransactionState,
                               val topicPartitions: mutable.Set[TopicAndPartition],
                               val txnStartTimestamp: Long,
                               val txnLastUpdateTimestamp: Long) {
  def prepareAddPartitions(addedTopicPartitions: List[TopicAndPartition], timestamp:Long):TransactionMetadata = {
    TransactionMetadata(transactionalId, producerId, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing,
      topicPartitions = topicPartitions ++ addedTopicPartitions,
      txnStartTimestamp, timestamp)
  }

  def prepareCommit() = {
    TransactionMetadata(transactionalId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, PrepareCommit,
      topicPartitions = topicPartitions,
      txnStartTimestamp,
      txnLastUpdateTimestamp)
  }

  def completeCommit() = {
    TransactionMetadata(transactionalId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs, CompleteCommit,
      topicPartitions = collection.mutable.Set.empty[TopicAndPartition], //clear transaction metadata.
      txnStartTimestamp,
      txnLastUpdateTimestamp)
  }

}
