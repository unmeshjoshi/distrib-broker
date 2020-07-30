package com.dist.consensus.network

import com.dist.consensus.HeartBeatScheduler

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
