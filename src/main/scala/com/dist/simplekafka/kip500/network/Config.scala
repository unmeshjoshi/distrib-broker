package com.dist.simplekafka.kip500.network

import java.io.File

import com.dist.simplekafka.network.InetAddressAndPort

case class Config(serverId: Long, serverAddress: InetAddressAndPort, peerConfig:List[Peer], walDir:File) {
  def clusterSize(): Int = peerConfig.size

  def getPeers() = {
    peerConfig.filter(p ⇒ p.id != serverId)
  }
}

