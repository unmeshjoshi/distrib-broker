package com.dist.mykafka

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import com.dist.common.TestUtils
import com.dist.simplekafka.network.InetAddressAndPort
import com.dist.util.Networks
import org.scalatest.FunSuite

class ZeroCopyReadServerTest extends FunSuite {

  test("should read messages from partition with zero copy") {
    //create partition and append few messages
    val p = new Partition("topic1", 0, TestUtils.tempDir())
    assert(p.logFile.exists())
    p.append("k1", "m1".getBytes)
    p.append("k2", "m2".getBytes)

    //start a socket server to accept socket connections to read from the partition
    val serverSocketAddress = startPartitionSocketServer(p)

    val clientSocket = connectTo(serverSocketAddress)
    val messages = read(clientSocket)

    assert(messages.size == 2)

    assert(messages(0) == "m1")
    assert(messages(1) == "m2")

  }

  private def connectTo(address: InetAddressAndPort) = {
    val clientSocket: SocketChannel = SocketChannel.open()
    clientSocket.connect(new InetSocketAddress(address.address, address.port))
    clientSocket
  }

  private def startPartitionSocketServer(p: Partition) = {
    val address = InetAddressAndPort(new Networks().ipv4Address, TestUtils.choosePort())
    val socketServer = new ZeroCopyReadServer(address, p)
    socketServer.start()

    TestUtils.waitUntilTrue(() => {
      socketServer.serverSocketChannel != null && socketServer.serverSocketChannel.socket().isBound
    }, "waiting till the server starts listening for client connections")
    address
  }

  private def read(channel: SocketChannel) = {
    List(readSingleMessage(channel),readSingleMessage(channel))
  }

  def readSingleMessage(channel:SocketChannel) = {
    val recordSize = readLength(channel)
    val messageBytes = readMessage(channel, recordSize)
    val message = new Message(messageBytes)
    message.readKey()
    message.readMessageBody()
  }

  class Message(byteBuffer: ByteBuffer) {
    def readKey(): Unit = {
      val keySize = byteBuffer.getInt
      val key = new Array[Byte](keySize)
      byteBuffer.get(key)
    }

    def readMessageBody() = {
      val messageSize = byteBuffer.getInt()
      val messageBytes = new Array[Byte](messageSize)
      byteBuffer.get(messageBytes)
      new String(messageBytes)
    }

  }

  private def readLength(channel: SocketChannel) = {
    val length = ByteBuffer.allocate(4)
    channel.read(length)
    length.flip()
    val recordSize = length.getInt()
    recordSize
  }

  private def readMessage(channel: SocketChannel, recordSize: Int) = {
    val records = ByteBuffer.allocate(recordSize);
    channel.read(records)
    records.flip()
    records
  }
}
