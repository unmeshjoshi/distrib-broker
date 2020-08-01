package com.dist.simplekafka.kip500.network

import com.dist.simplekafka.kip500.HeartBeatScheduler
import com.dist.simplekafka.network.InetAddressAndPort

case class Peer(id:Int, address:InetAddressAndPort)


case class PeerProxy(peerInfo: Peer, var matchIndex: Long = 0, heartbeatSender: PeerProxy ⇒ Unit) {
  val heartBeat = new HeartBeatScheduler(heartbeatSenderWrapper)

  def heartbeatSenderWrapper() = {
    heartbeatSender(this)
  }

  def start(): Unit = {
    heartBeat.start()
  }

  def stop()= {
    heartBeat.cancel()
  }
}
