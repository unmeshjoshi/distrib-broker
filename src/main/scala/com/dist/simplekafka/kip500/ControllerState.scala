package com.dist.simplekafka.kip500

import java.io.{ByteArrayInputStream, File}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.util
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.stream.Collectors
import java.util.{Map, stream}

import akka.util.Timeout

case class ClientSession(lastModifiedTime: Long, clientId: String, responses: util.Map[Int, String]) {

}

class ControllerState(walDir: File) {
  val kv = new mutable.HashMap[String, String]()
  val wal = WriteAheadLog.create(walDir)
  applyLog()
  val activeBrokers = new ConcurrentHashMap[String, Lease]


  var leaseTracker = new FollowerLeaseTracker(activeBrokers)

  def put(key: String, value: String): Unit = {
    wal.writeEntry(SetValueCommand(key, value).serialize())
  }

  def get(key: String): Option[String] = kv.get(key)



  def close = {
    kv.clear()
  }


  def applyEntries(entries: List[WalEntry]): Unit = {
    entries.foreach(entry ⇒ {
      applyEntry(entry)
    })
  }



  def applyEntry(entry: WalEntry) = {
    if (entry.entryType == EntryType.data) {
      val command = Command.deserialize(new ByteArrayInputStream(entry.data))
      command match {
        case setValueCommand: SetValueCommand => {
            kv.put(setValueCommand.key, setValueCommand.value)
        }
        case registerClientCommand: BrokerHeartbeat => {
          val brokerId = if (registerClientCommand.brokerId.isEmpty) s"${entry.entryId}" else registerClientCommand.brokerId
          activeBrokers.put(brokerId, new Lease(brokerId, 1000))
          brokerId
        }
      }
    }
  }

  val sessionTimeoutNanos = TimeUnit.SECONDS.toNanos(1)

  def applyLog() = {
    val entries: List[WalEntry] = wal.readAll().toList
    applyEntries(entries)
  }
}
