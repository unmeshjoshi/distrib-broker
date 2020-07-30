package com.dist.simplekafka.network

import java.net.Socket

import com.dist.simplekafka.api.RequestOrResponse
import com.dist.util.SocketIO

class SocketSender {

  def sendReceiveTcp(message: RequestOrResponse, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[RequestOrResponse](clientSocket, classOf[RequestOrResponse]).requestResponse(message)
  }
}
