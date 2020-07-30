package com.dist.simplekafka.kip500

import java.io.ByteArrayInputStream

import com.dist.simplekafka.kip500.network.{Config, InetAddressAndPort, Peer}
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration._

class Kip500ControllerTest extends FunSuite {

    test("basic test to update commitIndex after quorum writes") {
      val address = new Networks().ipv4Address
      val ports = TestUtils.choosePorts(3)
      val peerAddr1 = InetAddressAndPort(address, ports(0))
      val peerAddr2 = InetAddressAndPort(address, ports(1))
      val peerAddr3 = InetAddressAndPort(address, ports(2))


      val serverList = List(Peer(1, peerAddr1), Peer(2, peerAddr2), Peer(3, peerAddr3))

      val config1 = Config(1, peerAddr1, serverList, TestUtils.tempDir())
      val peer1 = new Controller(config1)

      val config2 = Config(2, peerAddr2, serverList, TestUtils.tempDir())
      val peer2 = new Controller(config2)

      val config3 = Config(3, peerAddr3, serverList, TestUtils.tempDir())
      val peer3 = new Controller(config3)

      peer1.startListening()
      peer2.startListening()
      peer3.startListening()

      peer1.start()
      peer2.start()
      peer3.start()

      TestUtils.waitUntilTrue(()⇒ {
        peer3.state == ServerState.LEADING && peer1.state == ServerState.FOLLOWING && peer2.state == ServerState.FOLLOWING
      }, "Waiting for leader to be selected")

      val future = peer3.put("k1", "v1")
      Await.ready(future, 5.second)
      val value = peer3.get("k1")
      assert(value == Some("v1"))
    }


  test("should register new broker with broker heartbeat") {
    val address = new Networks().ipv4Address
    val ports = TestUtils.choosePorts(3)
    val peerAddr1 = InetAddressAndPort(address, ports(0))
    val peerAddr2 = InetAddressAndPort(address, ports(1))
    val peerAddr3 = InetAddressAndPort(address, ports(2))


    val serverList = List(Peer(1, peerAddr1), Peer(2, peerAddr2), Peer(3, peerAddr3))

    val config1 = Config(1, peerAddr1, serverList, TestUtils.tempDir())
    val peer1 = new Controller(config1)

    val config2 = Config(2, peerAddr2, serverList, TestUtils.tempDir())
    val peer2 = new Controller(config2)

    val config3 = Config(3, peerAddr3, serverList, TestUtils.tempDir())
    val activeController = new Controller(config3)

    peer1.startListening()
    peer2.startListening()
    activeController.startListening()

    peer1.start()
    peer2.start()
    activeController.start()

    TestUtils.waitUntilTrue(()⇒ {
      activeController.state == ServerState.LEADING && peer1.state == ServerState.FOLLOWING && peer2.state == ServerState.FOLLOWING
    }, "Waiting for leader to be selected")

    val future = activeController.brokerHeartbeat(BrokerHeartbeat("0"))
    Await.ready(future, 5.second)
    val value = activeController.kv.activeBrokers.get("0")
    assert(value.getName == "0")
  }

  test("should commit FenceBroker record when broker lease expires") {
    val address = new Networks().ipv4Address
    val ports = TestUtils.choosePorts(3)
    val peerAddr1 = InetAddressAndPort(address, ports(0))
    val peerAddr2 = InetAddressAndPort(address, ports(1))
    val peerAddr3 = InetAddressAndPort(address, ports(2))


    val serverList = List(Peer(1, peerAddr1), Peer(2, peerAddr2), Peer(3, peerAddr3))

    val config1 = Config(1, peerAddr1, serverList, TestUtils.tempDir())
    val peer1 = new Controller(config1)

    val config2 = Config(2, peerAddr2, serverList, TestUtils.tempDir())
    val peer2 = new Controller(config2)

    val config3 = Config(3, peerAddr3, serverList, TestUtils.tempDir())
    val peer3 = new Controller(config3)

    peer1.startListening()
    peer2.startListening()
    peer3.startListening()

    peer1.start()
    peer2.start()
    peer3.start()

    TestUtils.waitUntilTrue(()⇒ {
      peer3.state == ServerState.LEADING && peer1.state == ServerState.FOLLOWING && peer2.state == ServerState.FOLLOWING
    }, "Waiting for leader to be selected")

    val activeController = peer3

    val future = activeController.brokerHeartbeat(BrokerHeartbeat("0"))
    Await.ready(future, 5.second)
    val controllerWal = activeController.kv.wal

    val walEntryId = controllerWal.lastLogEntryId

    val value = activeController.kv.activeBrokers.get("0")
    assert(value.getName == "0")

    TestUtils.waitUntilTrue(()=>{
      activeController.kv.activeBrokers.size() == 0
    }, "waiting for broker lease to expire")

    val entries = controllerWal.entries(walEntryId, controllerWal.lastLogEntryId)
    assert(entries.size == 1)
    val data = entries(0).data
    val command = Command.deserialize(new ByteArrayInputStream(data))
    assert(command.asInstanceOf[FenceBroker].clientId == "0")
  }

  test("create topic should commit topicrecord and partitionrecord") {
    val address = new Networks().ipv4Address
    val ports = TestUtils.choosePorts(3)
    val peerAddr1 = InetAddressAndPort(address, ports(0))
    val peerAddr2 = InetAddressAndPort(address, ports(1))
    val peerAddr3 = InetAddressAndPort(address, ports(2))


    val serverList = List(Peer(1, peerAddr1), Peer(2, peerAddr2), Peer(3, peerAddr3))

    val config1 = Config(1, peerAddr1, serverList, TestUtils.tempDir())
    val peer1 = new Controller(config1)

    val config2 = Config(2, peerAddr2, serverList, TestUtils.tempDir())
    val peer2 = new Controller(config2)

    val config3 = Config(3, peerAddr3, serverList, TestUtils.tempDir())
    val peer3 = new Controller(config3)

    peer1.startListening()
    peer2.startListening()
    peer3.startListening()

    peer1.start()
    peer2.start()
    peer3.start()

    TestUtils.waitUntilTrue(()⇒ {
      peer3.state == ServerState.LEADING && peer1.state == ServerState.FOLLOWING && peer2.state == ServerState.FOLLOWING
    }, "Waiting for leader to be selected")

    val activeController = peer3

    val future = activeController.brokerHeartbeat(BrokerHeartbeat("0"))
    Await.ready(future, 5.second)

    val future2 = activeController.brokerHeartbeat(BrokerHeartbeat("1"))
    Await.ready(future2, 5.second)

    val future3 = activeController.brokerHeartbeat(BrokerHeartbeat("2"))
    Await.ready(future3, 5.second)

    val controllerWal = activeController.kv.wal

    val walEntryId = controllerWal.lastLogEntryId

    TestUtils.waitUntilTrue(()=>{
      activeController.kv.activeBrokers.size() == 3
    }, "waiting for broker lease to expire")

    activeController.createTopic("topic1", 2, 2)
    
  }

}
