package com.dist.simplekafka.kip500

import java.net.Socket

import com.dist.simplekafka.kip500.network.{InetAddressAndPort, RequestOrResponse, SocketIO}

class NetworkClient extends Logging {
  def sendReceive(requestOrResponse: RequestOrResponse, to: InetAddressAndPort): RequestOrResponse = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[RequestOrResponse](clientSocket, classOf[RequestOrResponse]).requestResponse(requestOrResponse)
  }
}
