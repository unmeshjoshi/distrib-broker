package com.dist.simplekafka.kip500

import java.io.{ByteArrayInputStream, File}
import java.util
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class ClientSession(lastModifiedTime: Long, clientId: String, responses: util.Map[Int, String]) {

}

class ControllerState() extends Logging {

  val activeBrokers = new ConcurrentHashMap[Int, Lease]
  var leaseTracker: LeaseTracker = new FollowerLeaseTracker(activeBrokers)

  def applyEntries(entries: List[WalEntry]): Unit = {
    entries.foreach(entry ⇒ {
      applyEntry(entry)
    })
  }


  def applyEntry(entry: WalEntry) = {
    if (entry.entryType == EntryType.data) {
      val command = Record.deserialize(new ByteArrayInputStream(entry.data))
      command match {
        case brokerHeartbeat: BrokerHeartbeat => {
          val brokerId = brokerHeartbeat.brokerId
          info(s"Registering Active Broker with id ${brokerId}")
          leaseTracker.addLease(new Lease(brokerId, TimeUnit.MILLISECONDS.toNanos(brokerHeartbeat.ttl)))
          brokerId
        }
        case topicRecord: TopicRecord => {
          println(topicRecord)
        }
        case partitionRecord: PartitionRecord => {
          println(partitionRecord)
        }
        case fenceBroker: FenceBroker => {
          println(fenceBroker)

        }
      }
    }
  }

  val sessionTimeoutNanos = TimeUnit.SECONDS.toNanos(1)

  def getActiveBrokerIds() = {
    activeBrokers.keys().asScala.map(_.toInt)
  }

}
