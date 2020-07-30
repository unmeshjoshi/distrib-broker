package com.dist.consensus

import com.dist.consensus.election.Vote
import com.dist.consensus.network.{Config, InetAddressAndPort, Peer}
import org.scalatest.FunSuite

class ServerTest extends FunSuite {

  test("should initialize to LOOKING state") {
    val address = new Networks().ipv4Address
    val ports = TestUtils.choosePorts(3)
    val peerAddr1 = InetAddressAndPort(address, ports(0))
    val peerAddr2 = InetAddressAndPort(address, ports(1))
    val peerAddr3 = InetAddressAndPort(address, ports(2))


    val serverList = List(Peer(1, peerAddr1), Peer(2, peerAddr2), Peer(3, peerAddr3))

    val config = Config(1, peerAddr1, serverList, TestUtils.tempDir())
    val server = new Server(config)
    assert(server.state == ServerState.LOOKING)
  }

  test("should start leader election in LOOKING state") {
    val address = new Networks().ipv4Address
    val ports = TestUtils.choosePorts(3)
    val peerAddr1 = InetAddressAndPort(address, ports(0))
    val peerAddr2 = InetAddressAndPort(address, ports(1))
    val peerAddr3 = InetAddressAndPort(address, ports(2))


    val serverList = List(Peer(1, peerAddr1), Peer(2, peerAddr2), Peer(3, peerAddr3))

    val config = Config(1, peerAddr1, serverList, TestUtils.tempDir())

    val server = new Server(config)
    assert(server.state == ServerState.LOOKING)

    server.start()
  }

  test("should vote to self at startup") {
    val address = new Networks().ipv4Address
    val ports = TestUtils.choosePorts(3)
    val peerAddr1 = InetAddressAndPort(address, ports(0))
    val peerAddr2 = InetAddressAndPort(address, ports(1))
    val peerAddr3 = InetAddressAndPort(address, ports(2))


    val serverList = List(Peer(1, peerAddr1), Peer(2, peerAddr2), Peer(3, peerAddr3))

    val config = Config(1, peerAddr1, serverList, TestUtils.tempDir())

    val server = new Server(config)
    assert(server.currentVote.get() == Vote(config.serverId, server.kv.wal.lastLogEntryId))
  }
}
