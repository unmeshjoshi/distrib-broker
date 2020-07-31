package com.dist.simplekafka.kip500

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object WriteAheadLog {
  val logSuffix = ".log"
  val logPrefix = "wal"
  val firstLogId = 0
  val sizeOfInt = 4
  val sizeOfLong = 8

  def create(walDir: File): WriteAheadLog = {
    val newLogFile = new File(walDir, logFileName())
    val file = new RandomAccessFile(newLogFile, "rw")
    val channel = file.getChannel
    new WriteAheadLog(channel)
  }

  def logFileName() = s"${logPrefix}-${firstLogId}${logSuffix}"


  def newBuffer(size: Int): ByteBuffer = {
    val buf: ByteBuffer = ByteBuffer.allocate(size)
    buf.clear
  }
}

class WriteAheadLog(fileChannel: FileChannel) extends Logging {
  var highWaterMark: Long = 0

  def truncate(logIndex: Long) = {
    val filePosition: Option[Long] = entryOffsets.get(logIndex)
    filePosition.map(position ⇒ {
      fileChannel.truncate(position)
      fileChannel.force(true)
      position
    }).orElse(Some(0L))

  }

  val entryOffsets = new mutable.HashMap[Long, Long]()
  entryOffsets.put(0, 0)

  var lastLogEntryId = 0L

  //should be used only on leader.
  def writeEntry(bytes: Array[Byte], entryType: Int = EntryType.data): Long = {
    this.synchronized {
      val logEntryId = lastLogEntryId + 1
      val logEntry = WalEntry(logEntryId, bytes, entryType, System.nanoTime())
      writeEntry(logEntry)
    }
  }

  def writeEntry(logEntry: WalEntry): Long = {
    this.synchronized {
      val buffer = logEntry.serialize()
      fileChannel.position(fileChannel.size())
      info(s"Writing walentry ${logEntry} => ${fileChannel.position()}")
      writeToChannel(buffer)
      lastLogEntryId = logEntry.entryId
      entryOffsets.put(logEntry.entryId, fileChannel.position())
      lastLogEntryId
    }
  }

  private def writeToChannel(buffer: ByteBuffer) = {
    buffer.flip()
    while (buffer.hasRemaining) {
      fileChannel.write(buffer)
    }
    fileChannel.force(true)
    fileChannel.position()
  }

  def close() = fileChannel.close()

  def readAll() = {
    //start from the beginning
    fileChannel.position(0)

    val entries = new scala.collection.mutable.ListBuffer[WalEntry]
    var totalBytesRead = 0L
    val deser = new WalEntryDeserializer(fileChannel)
    while (totalBytesRead < fileChannel.size()) {
      val (logEntry, bytesRead, position) = deser.readEntry()
      entries += logEntry
      totalBytesRead = totalBytesRead + bytesRead
      entryOffsets.put(logEntry.entryId, position)
    }
    lastLogEntryId = if (entries.isEmpty) 0 else entries.last.entryId
    entries
  }


  def entries(from: Long, to: Long) = {
    this.synchronized {
      val entries = new scala.collection.mutable.ListBuffer[WalEntry]
      var totalBytesRead = 0L
      val deser = new WalEntryDeserializer(fileChannel)
      val startOffset: Option[Long] = entryOffsets.get(from)
      fileChannel.position(startOffset.get)
      info(s"Reading wal entries from ${from} to ${to}. Entries in WAL are ${this.entryOffsets} HW=${this.highWaterMark} ")
      var (logEntry, bytesRead, position) = deser.readEntry()
      entries += logEntry
      var fromEntryId = logEntry.entryId
      while (fromEntryId < to) {
        val e: WalEntry = readEntry(deser)
        entries += e
        fromEntryId = e.entryId
      }
      entries
    }
  }

  private def readEntry(deser: WalEntryDeserializer) = {
    val (logEntry, bytesRead, position) = deser.readEntry()
    logEntry
  }
}
