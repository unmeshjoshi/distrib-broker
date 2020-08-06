package com.dist.simplekafka.kip500

import com.dist.simplekafka.kip500.network.{Config, Peer}
import com.dist.simplekafka.network.InetAddressAndPort
import org.scalatest.FunSuite

class LeaderElectionTest extends FunSuite {

  test("should elect server with highest id as a leader when logs are empty") {
    val address = new Networks().ipv4Address
    val ports = TestUtils.choosePorts(3)
    val peerAddr1 = InetAddressAndPort(address, ports(0))
    val peerAddr2 = InetAddressAndPort(address, ports(1))
    val peerAddr3 = InetAddressAndPort(address, ports(2))


    val serverList = List(Peer(1, peerAddr1), Peer(2, peerAddr2), Peer(3, peerAddr3))

    val config1 = Config(1, peerAddr1, serverList, TestUtils.tempDir())
    val peer1 = new Kip631Controller(config1)

    val config2 = Config(2, peerAddr2, serverList, TestUtils.tempDir())
    val peer2 = new Kip631Controller(config2)

    val config3 = Config(3, peerAddr3, serverList, TestUtils.tempDir())
    val peer3 = new Kip631Controller(config3)

    peer1.startListening()
    peer2.startListening()
    peer3.startListening()

    peer1.start()
    peer2.start()
    peer3.start()

    TestUtils.waitUntilTrue(()⇒ {
      peer3.consensus.getState() == ServerState.LEADING && peer1.consensus.getState() == ServerState.FOLLOWING && peer2.consensus.getState() == ServerState.FOLLOWING
    }, "Waiting for leader to be selected")

  }

}
