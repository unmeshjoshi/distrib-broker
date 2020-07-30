package com.dist.mykafka

import java.io.{File, RandomAccessFile}
import java.util
import java.util.concurrent.atomic.AtomicInteger

class Partition(val topic:String, val partition:Int, logDir:File) {
  val offset = new AtomicInteger()
  val offsetMap = new util.HashMap[Int, Long]()

  def append(str: String, msg: Array[Byte]) = {
    logRW.seek(logRW.length())
    val startPosition = logRW.getFilePointer
    logRW.writeUTF(str)
    logRW.writeInt(msg.length)
    logRW.write(msg)

    offsetMap.put(offset.incrementAndGet(), startPosition)
    offset.get()
  }

  def read(offset:Int) = {
    val fileLocation = offsetMap.get(offset)
    logRW.seek(fileLocation)

    val key = logRW.readUTF()
    val size = logRW.readInt()
    val messageBytes = new Array[Byte](size)
    logRW.read(messageBytes)
    List(new String(messageBytes))
  }

  val logFile = new File(logDir, s"${topic}_${partition}.log")
  logFile.createNewFile()
  val logRW = new RandomAccessFile(logFile, "rw")
}
