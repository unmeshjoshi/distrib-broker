package com.dist.mykafka

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicInteger

class Partition(val topic:String, val partition:Int, logDir:File) {
  val logFile = new File(logDir, s"${topic}_${partition}.log")
  logFile.createNewFile()
  val logRW = new RandomAccessFile(logFile, "rw")
  val channel = logRW.getChannel

  val offset = new AtomicInteger()
  val offsetMap = new util.HashMap[Int, Long]()

  def append(key: String, messageBytes: Array[Byte]) = {
    val startPosition = channel.position()
    val keyBytes = key.getBytes()
    writeTotalMessageLength(messageBytes, keyBytes)
    writeKey(keyBytes)
    writeMessage(messageBytes)

    offsetMap.put(offset.incrementAndGet(), startPosition)
    offset.get()
  }

  private def writeMessage(messageBytes: Array[Byte]) = {
    val lengthBuffer2 = ByteBuffer.allocate(4)
    lengthBuffer2.putInt(messageBytes.length)
    lengthBuffer2.flip()
    channel.write(lengthBuffer2)

    val msgBuff = ByteBuffer.allocate(messageBytes.length)
    msgBuff.put(messageBytes)
    msgBuff.flip()
    channel.write(msgBuff)
  }

  private def writeKey(keyBytes: Array[Byte]) = {
    val lengthBuffer = ByteBuffer.allocate(4)
    lengthBuffer.putInt(keyBytes.size)
    lengthBuffer.flip()
    channel.write(lengthBuffer)

    val keyBuffer = ByteBuffer.allocate(keyBytes.length)
    keyBuffer.put(keyBytes)
    keyBuffer.flip()
    channel.write(keyBuffer)
  }

  private def writeTotalMessageLength(msg: Array[Byte], keyBytes: Array[Byte]) = {
    val recordLendthBuffer = ByteBuffer.allocate(4)
    recordLendthBuffer.putInt(4 + 4 + keyBytes.size + msg.length)
    recordLendthBuffer.flip()
    channel.write(recordLendthBuffer)
  }

  def readMessageSet(offset:Int):FileMessageSet = {
    val messagePosition = offsetMap.get(offset)
    new FileMessageSet(logFile, logRW.getChannel, messagePosition, channel.size())
  }

  def read(offset:Int) = {
    val fileLocation = offsetMap.get(offset)
    readSingleMessage(fileLocation)
  }

  def readSingleMessage(fileLocation: Long) = {
    val recordSize: Int = readLength(fileLocation)
    val message: ByteBuffer = readMessage(fileLocation, recordSize)

    val keyBytes = readKey(message)
    val messageBytes = readMessageValue(message)

    List(new String(messageBytes))
  }

  private def readMessageValue(message: ByteBuffer) = {
    val messageSize = message.getInt()
    val messageBytes = new Array[Byte](messageSize)
    message.get(messageBytes)
    messageBytes
  }

  private def readKey(message: ByteBuffer) = {
    val keySize = message.getInt
    val key = new Array[Byte](keySize)
    message.get(key)
    key
  }

  private def readMessage(fileLocation: Long, recordSize: Int) = {
    val records = ByteBuffer.allocate(recordSize);
    channel.read(records, fileLocation.toInt + 4)
    records.flip()
    records
  }

  private def readLength(fileLocation: Long) = {
    val length = ByteBuffer.allocate(4)
    channel.read(length, fileLocation)
    length.flip()
    val recordSize = length.getInt()
    recordSize
  }
}
