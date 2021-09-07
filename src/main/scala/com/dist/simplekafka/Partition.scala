package com.dist.simplekafka

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import com.dist.simplekafka.api.{RequestKeys, RequestOrResponse}
import com.dist.simplekafka.common.{JsonSerDes, Logging, TopicAndPartition}
import com.dist.simplekafka.network.InetAddressAndPort
import com.dist.simplekafka.server.Config
import com.dist.simplekafka.util.ZkUtils.Broker

import java.io._
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

sealed trait FetchIsolation

case object FetchLogEnd extends FetchIsolation

case object FetchHighWatermark extends FetchIsolation

case object FetchTxnCommitted extends FetchIsolation

class Partition(config: Config, topicAndPartition: TopicAndPartition)(implicit system: ActorSystem) extends Logging {
  private val lock = new ReentrantLock()

  val producerStateManager = new ProducerStateManager(topicAndPartition)
  val replicaOffsets = new util.HashMap[Int, Long]
  var highWatermark = 0l;

  val LogFileSuffix = ".log"
  val logFile =
    new File(config.logDirs(0), topicAndPartition.topic + "-" + topicAndPartition.partition + LogFileSuffix)

  val sequenceFile = new SequenceFile(config)
  val reader = new sequenceFile.Reader(logFile.getAbsolutePath)
  val writer = new sequenceFile.Writer(logFile.getAbsolutePath)

  case class BrokerAndFetcherId(broker: Broker, fetcherId: Int)

  val numFetchers = 1
  private val fetcherThreadMap = new mutable.HashMap[BrokerAndFetcherId, ReplicaFetcherThread]

  private def getFetcherId(topicAndPartition: TopicAndPartition): Int = {
    (topicAndPartition.topic.hashCode() + 31 * topicAndPartition.partition) //% numFetchers
  }

  def createFetcherThread(fetcherId: Int, leaderBroker: Broker): ReplicaFetcherThread = {
    new ReplicaFetcherThread("ReplicaFetcherThread-%d-%d".format(fetcherId, leaderBroker.id), leaderBroker, this, config)
  }

  def addFetcher(topicAndPartition: TopicAndPartition, initialOffset: Long, leaderBroker: Broker) {
    var fetcherThread: ReplicaFetcherThread = null
    val key = BrokerAndFetcherId(leaderBroker, getFetcherId(topicAndPartition))
    fetcherThreadMap.get(key) match {
      case Some(f) => fetcherThread = f
      case None =>
        fetcherThread = createFetcherThread(key.fetcherId, leaderBroker)
        fetcherThreadMap.put(key, fetcherThread)
        fetcherThread.start
    }
    fetcherThread.addPartition(topicAndPartition, initialOffset)
    info("Adding fetcher for partition [%s,%d], initOffset %d to broker %d with fetcherId %d"
      .format(topicAndPartition.topic, topicAndPartition.partition, initialOffset, leaderBroker.id, key.fetcherId))
  }

  def makeFollower(leader: Broker) = {
    addFetcher(topicAndPartition, sequenceFile.lastOffset(), leader)
  }

  def makeLeader() = {
    //stop fetcher threads.
  }


  object PartitionTopicInfo {
    val InvalidOffset = -1L

    def isOffsetInvalid(offset: Long) = offset < 0L
  }

  class ReplicaFetcherThread(name: String,
                             leaderBroker: Broker,
                             parition: Partition, config: Config) extends Thread with Logging {

    var topicPartitions = new ListBuffer[TopicAndPartition]()

    def addPartition(topicAndPartition: TopicAndPartition, initialOffset: Long) {
      topicPartitions += topicAndPartition
    }

    val isRunning: AtomicBoolean = new AtomicBoolean(true)
    val correlationId = new AtomicInteger(0)
    val socketClient = new SocketClient

    def doWork(): Unit = {
      parition.sequenceFile.lastOffset();
      if (!topicPartitions.isEmpty) {
        val topicPartition = topicPartitions(0) //expect only only for now.
        val consumeRequest = ConsumeRequest(topicPartition, FetchLogEnd.toString, parition.sequenceFile.lastOffset() + 1, config.brokerId)
        val request = RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(consumeRequest), correlationId.getAndIncrement())
        val response = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(leaderBroker.host, leaderBroker.port))
        val consumeResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[ConsumeResponse])
        consumeResponse.messages.foreach(m ⇒ {
          info(s"Replicating message ${m} for topic partition ${topicPartition} in broker ${config.brokerId}")
          parition.append(m._1, m._2)
        })
      }
    }

    override def run(): Unit = {
      info("Starting ")
      try {
        while (isRunning.get()) {
          doWork()
        }
      } catch {
        case e: Throwable =>
          if (isRunning.get())
            error("Error due to ", e)
      }
      info("Stopped ")
    }
  }

  val source: Source[(String, String, Promise[Int]), ActorRef] = Source.actorRef(100, OverflowStrategy.dropHead)
  private val (actorRef, s) = source.preMaterialize()

  s.runForeach({ case (k, v, p) ⇒
    val index = append(k, v)
    p.success(index)
  })

  def append2(key: String, message: String) = {
    val p = Promise[Int]()
    actorRef ! (key, message, p)
    Await.result(p.future, 1.second)
  }

  def completeTransaction(writeTxnMarkersRequest: WriteTxnMarkersRequest) = {
    lock.lock()
    try {
      val endOffset = append(writeTxnMarkersRequest.transactionalId, JsonSerDes.serialize(writeTxnMarkersRequest))
      producerStateManager.completeTxn(writeTxnMarkersRequest.producerId, writeTxnMarkersRequest.transactionalId, endOffset)
    } finally {
      lock.unlock()
    }
  }


  def append(transactionalId:String, producerId:Long, key: String, message: String):Int = {
    lock.lock()
    try {
      val offset = append(key, message)
      producerStateManager.append(transactionalId, producerId, offset, key, message)
      maybeIncrementFirstUnstableOffset()
      offset
    } finally {
      lock.unlock()
    }
  }
  private var firstUnstableOffsetMetadata: Option[Long] = None


  private def fetchLastStableOffsetMetadata: Long = {
    // cache the current high watermark to avoid a concurrent update invalidating the range check
    val highWatermarkMetadata = highWatermark

    firstUnstableOffsetMetadata match {
      case Some(offsetMetadata) if offsetMetadata < highWatermarkMetadata =>
          offsetMetadata - 1
       case _ => highWatermarkMetadata
    }
  }

  def maybeIncrementFirstUnstableOffset() = {
    val updatedFirstStableOffset: Option[Long] = producerStateManager.firstUnstableOffset()

    if (updatedFirstStableOffset != this.firstUnstableOffsetMetadata) {
      info(s"First unstable offset updated to $updatedFirstStableOffset in" + config.brokerId)
      this.firstUnstableOffsetMetadata = updatedFirstStableOffset
    }
  }

  def append(key: String, message: String):Int = {
    val currentPos = writer.getCurrentPosition
    try {
      val offset = writer.append(key, message)
      offset
    }
    catch {
      case e: IOException =>
        writer.seek(currentPos)
        throw e
    }
  }

  private val remoteReplicasMap = new util.HashMap[Int, Long]


  def updateLastReadOffsetAndHighWaterMark(replicaId: Int, offset: Long) = {
    lock.lock()
    try {
      remoteReplicasMap.put(replicaId, offset)
      val values = remoteReplicasMap.values().asScala.toList.sorted
      if (highWatermark < values(0)) {
        highWatermark = values(0)
        info(s"Updated highwatermark to ${highWatermark} for ${this.topicAndPartition} on ${config.brokerId}")
        producerStateManager.removeUnreplicatedTransactions(highWatermark) //transaction records are replicated.
      }
      maybeIncrementFirstUnstableOffset()
    } finally {
      lock.unlock()
    }
  }


  def read(offset: Long = 0, replicaId: Int = -1, isolation: FetchIsolation = FetchLogEnd):List[Row] = {
    lock.lock()
    info("Reading partition "+ topicAndPartition + " for fetchIsolation " + isolation)
    try {
      val maxOffset: Long =
        if (isolation == FetchTxnCommitted) fetchLastStableOffsetMetadata
        else if (isolation == FetchHighWatermark) highWatermark
        else sequenceFile.lastOffset()

      if (offset > maxOffset) {
        return List[Row]()
      }

      val result = new java.util.ArrayList[Row]()
      val offsets: mutable.Set[Long] = sequenceFile.getAllOffSetsFrom(offset, maxOffset)

      offsets.foreach(offset ⇒ {
        val filePosition = sequenceFile.offsetIndexes.get(offset)

        val ba = new ByteArrayOutputStream()
        val baos = new DataOutputStream(ba)

        reader.seekToOffset(filePosition)
        reader.next(baos)

        val bais = new DataInputStream(new ByteArrayInputStream(ba.toByteArray))
        Try(Row.deserialize(bais)) match {
          case Success(row) => result.add(row)
          case Failure(exception) => None
        }
      })
      result.asScala.toList
    } finally {
      lock.unlock()
    }
  }


  object Row {
    def serialize(row: Row, dos: DataOutputStream): Unit = {
      dos.writeUTF(row.key)
      dos.writeInt(row.value.getBytes().size)
      dos.write(row.value.getBytes) //TODO: as of now only supporting string writes.
    }

    def deserialize(dis: DataInputStream): Row = {
      val key = dis.readUTF()
      val dataSize = dis.readInt()
      val bytes = new Array[Byte](dataSize)
      dis.read(bytes)
      val value = new String(bytes) //TODO:As of now supporting only string values
      Row(key, value)
    }
  }

  case class Row(key: String, value: String)

}
