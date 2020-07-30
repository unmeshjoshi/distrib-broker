package com.dist.consensus.network

import com.dist.consensus.{Networks, TestUtils}
import org.scalatest.FunSuite

class ConfigTest extends FunSuite {
  test("should get list of peers") {
    val address = new Networks().ipv4Address
    val ports = TestUtils.choosePorts(3)
    val peerAddr1 = InetAddressAndPort(address, ports(0))
    val peerAddr2 = InetAddressAndPort(address, ports(1))
    val peerAddr3 = InetAddressAndPort(address, ports(2))

    val serverList = List(Peer(1, peerAddr1), Peer(2, peerAddr2), Peer(3, peerAddr3))

    val config = Config(1, peerAddr1, serverList, TestUtils.tempDir())
    assert(config.getPeers() == List(Peer(2, peerAddr2), Peer(3, peerAddr3)))
  }

}
