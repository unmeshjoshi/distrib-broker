package com.dist.consensus.network

import java.io.File

case class Config(serverId: Long, serverAddress: InetAddressAndPort, peerConfig:List[Peer], walDir:File) {
  def clusterSize(): Int = peerConfig.size

  def getPeers() = {
    peerConfig.filter(p ⇒ p.id != serverId)
  }
}

