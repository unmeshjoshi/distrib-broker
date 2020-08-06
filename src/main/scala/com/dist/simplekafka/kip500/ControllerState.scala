package com.dist.simplekafka.kip500

import java.io.ByteArrayInputStream
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.jdk.CollectionConverters._


case class BrokerRegistrationResponse(brokerEpoch:Long, errorCode:Int) extends Response

class ControllerState() extends Logging {

  val activeBrokers = new ConcurrentHashMap[Int, Lease]
  var leaseTracker: LeaseTracker = new FollowerLeaseTracker(activeBrokers)

  def applyEntries(entries: List[WalEntry]): List[Response] = {
    entries.map(entry ⇒ {
      applyEntry(entry)
    })
  }

  def applyEntry(entry: WalEntry):Response = {
    if (entry.entryType == EntryType.data) {
      val command = Record.deserialize(new ByteArrayInputStream(entry.data))
      command match {
        case brokerHeartbeat: BrokerHeartbeat => {
          
          val brokerId = brokerHeartbeat.brokerId
          info(s"Registering Active Broker with id ${brokerId}")
          leaseTracker.addLease(new Lease(brokerId, TimeUnit.MILLISECONDS.toNanos(brokerHeartbeat.ttl)))
          val brokerEpoch = entry.entryId //brokerEpoch is log entry offset
          BrokerRegistrationResponse(brokerEpoch, 0)
        }
        case topicRecord: TopicRecord => {
          info(s"Applying ${topicRecord}")
          Response.None //TODO
        }
        case partitionRecord: PartitionRecord => {
          info(s"Applying ${partitionRecord}")
          Response.None
        }
        case fenceBroker: FenceBroker => {
          info(s"Applying ${fenceBroker}")
          Response.None
        }

      }
    } else Response.None


  }

  val sessionTimeoutNanos = TimeUnit.SECONDS.toNanos(1)

  def getActiveBrokerIds() = {
    activeBrokers.keys().asScala.map(_.toInt)
  }

}
