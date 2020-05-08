package org.dist.simplekafka

import java.net.Socket

import org.dist.simplekafka.api.RequestOrResponse
import org.dist.simplekafka.network.InetAddressAndPort
import org.dist.util.SocketIO

class SocketClient {

  def sendReceiveTcp(message: RequestOrResponse, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[RequestOrResponse](clientSocket, classOf[RequestOrResponse]).requestResponse(message)
  }
}
