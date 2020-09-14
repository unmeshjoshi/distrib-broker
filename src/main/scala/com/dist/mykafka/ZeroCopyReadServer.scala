package com.dist.mykafka

import java.net.InetSocketAddress
import java.nio.channels.ServerSocketChannel

import com.dist.simplekafka.common.Logging
import com.dist.simplekafka.kip500.Utils
import com.dist.simplekafka.network.InetAddressAndPort

class ZeroCopyReadServer(address:InetAddressAndPort, partition:Partition) extends Thread with Logging {
  var serverSocketChannel: ServerSocketChannel = _
  override def run() = {
    Utils.swallow({
      import java.nio.channels.ServerSocketChannel
      serverSocketChannel = ServerSocketChannel.open
      serverSocketChannel.bind(new InetSocketAddress(address.address, address.port))
      info(s"Listening on ${address}")
      while (true) {
        val clientSocketChannel = serverSocketChannel.accept()
        //Handling only read requests for demo.. directly reading message from offset 0
        val messageSet = partition.readMessageSet(0)
        messageSet.writeTo(clientSocketChannel)
      }
    }
    )
  }
}
