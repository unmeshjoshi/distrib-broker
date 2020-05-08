package org.dist.queue.network

import java.net.Socket

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.api.RequestOrResponse
import org.dist.util.SocketIO

class SocketSender {

  def sendReceiveTcp(message: RequestOrResponse, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[RequestOrResponse](clientSocket, classOf[RequestOrResponse]).requestResponse(message)
  }
}
