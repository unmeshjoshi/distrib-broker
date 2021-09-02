package com.dist.simplekafka

import com.dist.simplekafka.common.{Logging, TopicAndPartition}

import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class CompletedTxn(producerId: Long, firstOffset: Long, lastOffset: Long, isAborted: Boolean) {
  override def toString: String = {
    "CompletedTxn(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"isAborted=$isAborted)"
  }
}

private case class TxnMetadata(producerId: Long,
                               firstOffset: Long,
                               var lastOffset: Option[Long] = None) {

  override def toString: String = {
    "TxnMetadata(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset)"
  }
}

class ProducerStateManager(topicPartition: TopicAndPartition) extends Logging {
  def removeUnreplicatedTransactions(highWatermark: Long) = {
    val iterator = unreplicatedTxns.entrySet.iterator
    while (iterator.hasNext) {
      val txnEntry = iterator.next()
      val lastOffset = txnEntry.getValue.lastOffset
      if (lastOffset.exists(_ < highWatermark))
        info(s"txn markers at offset $lastOffset are replicated. Removing from producer state")
        iterator.remove()
    }
  }

  /**
   * The last written record for a given producer. The last data offset may be undefined
   * if the only log entry for a producer is a transaction marker.
   */
  case class LastRecord(lastDataOffset: Option[Long], producerEpoch: Short)


  // ongoing transactions sorted by the first offset of the transaction
  private val ongoingTxns = new util.TreeMap[Long, TxnMetadata]
  // completed transactions whose markers are at offsets above the high watermark
  private val unreplicatedTxns = new util.TreeMap[Long, TxnMetadata]
  private val producers = mutable.Map.empty[Long, ProducerStateEntry]

  def update(appendInfo: ProducerAppendInfo) = {
    val updatedEntry = appendInfo.toEntry
    producers.get(appendInfo.producerId) match {
      case Some(currentEntry) =>
        currentEntry.update(updatedEntry)

      case None =>
        producers.put(appendInfo.producerId, updatedEntry)
    }

    appendInfo.startedTransactions.foreach { txn =>
      ongoingTxns.put(txn.firstOffset, txn)
    }
  }


  def append(transactionalId:String, producerId:Long, offset:Long, key:String, message:String) = {
      //check if we are starting a new transaction
    val currentEntry = lastEntry(producerId).getOrElse(ProducerStateEntry.empty(producerId))
    val isControlMessage = false //TODO: for end transaction markers
    val producerAppendInfo = new ProducerAppendInfo(topicPartition, producerId, offset, currentEntry)
      .append(offset, message, transactionalId != "", isControlMessage)
    
    update(producerAppendInfo)
      //record offset as a first offset of the transaction.
      //if its a control record, move transaction to unreplicated
      //after highwatermark reaches unreplicated offset, complete the transaction.

  }

  def lastEntry(producerId: Long): Option[ProducerStateEntry] = producers.get(producerId)
  /**
   * An unstable offset is one which is either undecided (i.e. its ultimate outcome is not yet known),
   * or one that is decided, but may not have been replicated (i.e. any transaction which has a COMMIT/ABORT
   * marker written at a higher offset than the current high watermark).
   */
  def firstUnstableOffset(): Option[Long] = {
    val unreplicatedFirstOffset = Option(unreplicatedTxns.firstEntry).map(_.getValue.firstOffset)
    val undecidedFirstOffset = Option(ongoingTxns.firstEntry).map(_.getValue.firstOffset)
    if (unreplicatedFirstOffset.isEmpty)
      undecidedFirstOffset
    else if (undecidedFirstOffset.isEmpty)
      unreplicatedFirstOffset
    else if (undecidedFirstOffset.get < unreplicatedFirstOffset.get)
      undecidedFirstOffset
    else
      unreplicatedFirstOffset
  }
  /**
   * Mark a transaction as completed. We will still await advancement of the high watermark before
   * advancing the first unstable offset.
   */
  def completeTxn(producerId:Long, transactionalId:String, lastOffset:Long): Unit = {
    val producerEntry: ProducerStateEntry = producers.get(producerId).get
    val txnMetadata = ongoingTxns.remove(producerEntry.currentTxnFirstOffset.get)
    if (txnMetadata == null)
      throw new IllegalArgumentException(s"Attempted to complete transaction $transactionalId on partition $topicPartition " +
        s"which was not started")

    txnMetadata.lastOffset = Some(lastOffset)

    unreplicatedTxns.put(producerEntry.currentTxnFirstOffset.get, txnMetadata)
  }
}


private object ProducerStateEntry {
  private val NumBatchesToRetain = 5

  def empty(producerId: Long) = new ProducerStateEntry(producerId,
    -1,
    producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
    coordinatorEpoch = -1,
    currentTxnFirstOffset = None)
}

case class ProducerStateEntry(val producerId: Long,
                              var offset:Long,
                              var producerEpoch: Short,
                              var coordinatorEpoch: Int,
                              var currentTxnFirstOffset: Option[Long]) {
  def update(nextEntry: ProducerStateEntry) = {
    this.offset = nextEntry.offset
    this.coordinatorEpoch = nextEntry.coordinatorEpoch
    this.currentTxnFirstOffset = nextEntry.currentTxnFirstOffset
  }

}


private class ProducerAppendInfo(val topicPartition: TopicAndPartition,
                                      val producerId: Long,
                                      val firstOffsetMetadata:Long,
                                      val currentEntry: ProducerStateEntry) extends Logging {

  def toEntry: ProducerStateEntry = updatedEntry


  private val transactions = ListBuffer.empty[TxnMetadata]
  private val updatedEntry = ProducerStateEntry.empty(producerId)

  updatedEntry.producerEpoch = currentEntry.producerEpoch
  updatedEntry.coordinatorEpoch = currentEntry.coordinatorEpoch
  updatedEntry.currentTxnFirstOffset = currentEntry.currentTxnFirstOffset

  def append(offset:Long, message:String, isTransactional:Boolean, isControl:Boolean) = {
    val firstOffset = offset
    updatedEntry.offset = offset //TODO: Keep a list of 5 entries
    updatedEntry.currentTxnFirstOffset match {
      case Some(_) if !isTransactional =>
        // Received a non-transactional message while a transaction is active
        throw new InvalidTxnStateException(s"Expected transactional write from producer $producerId at " +
          s"offset $firstOffsetMetadata in partition $topicPartition")

      case None if isTransactional =>
        // Began a new transaction
        updatedEntry.currentTxnFirstOffset = Some(firstOffset)
        transactions += TxnMetadata(producerId, firstOffsetMetadata)

      case _ => // nothing to do
    }
    this
  }

  def startedTransactions: List[TxnMetadata] = transactions.toList
}