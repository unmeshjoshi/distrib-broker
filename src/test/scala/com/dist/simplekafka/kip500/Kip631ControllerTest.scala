package com.dist.simplekafka.kip500

import java.io.ByteArrayInputStream

import com.dist.simplekafka.kip500.network.{Config, Peer}
import com.dist.simplekafka.network.InetAddressAndPort
import org.scalatest.FunSuite

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class Kip631ControllerTest extends FunSuite {

  test("should register new broker with broker heartbeat") {
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

    //with the election algorithm in Leader which selects leader based on ids. Controller with id=3 will be leader
    val activeController = new Kip631Controller(config3)

    peer1.startListening()
    peer2.startListening()
    activeController.startListening()

    peer1.start()
    peer2.start()
    activeController.start()

    TestUtils.waitUntilTrue(()⇒ {
      activeController.consensus.getState() == ServerState.LEADING && peer1.consensus.getState() == ServerState.FOLLOWING && peer2.consensus.getState() == ServerState.FOLLOWING
    }, "Waiting for leader to be selected")

    val future = activeController.brokerHeartbeat(BrokerHeartbeat(0, BrokerState.FENCED, BrokerState.ACTIVE, InetAddressAndPort(address, 8080), 2000))
    Await.ready(future, 5.second)
    val value = activeController.controllerState.activeBrokers.get(0)
    assert(value.getName == 0)
  }

  test("should commit FenceBroker record when broker lease expires") {
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

    val activeController = peer3

    val future = activeController.brokerHeartbeat(BrokerHeartbeat(0, BrokerState.ACTIVE, BrokerState.ACTIVE, InetAddressAndPort(address, 8080), 2000))
    Await.ready(future, 5.second)

    val value = activeController.controllerState.activeBrokers.get(0)
    assert(value.getName == 0)

    TestUtils.waitUntilTrue(()=>{
      activeController.consensus.readEntries(0).size == 2
    }, "waiting for broker lease to expire", 5000)

    val entries = activeController.consensus.readEntries(0)
    assert(entries.size == 2)
    val data = entries(1).data
    val command = Record.deserialize(new ByteArrayInputStream(data))
    assert(command.asInstanceOf[FenceBroker].clientId == 0)
  }

  test("create topic should commit topicrecord and partitionrecord") {
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

    val activeController = peer3

    val brokerPorts = TestUtils.choosePorts(3)

    val future = activeController.brokerHeartbeat(BrokerHeartbeat(0,BrokerState.ACTIVE, BrokerState.ACTIVE, InetAddressAndPort(address, brokerPorts(0)), 2000))
    Await.ready(future, 5.second)

    val future2 = activeController.brokerHeartbeat(BrokerHeartbeat(1, BrokerState.ACTIVE, BrokerState.ACTIVE,InetAddressAndPort(address, brokerPorts(1)), 2000))
    Await.ready(future2, 5.second)

    val future3: Future[Any] = activeController.brokerHeartbeat(BrokerHeartbeat(2, BrokerState.ACTIVE, BrokerState.ACTIVE,InetAddressAndPort(address, brokerPorts(2)), 2000))
    Await.ready(future3, 5.second)

    TestUtils.waitUntilTrue(()=>{
      activeController.controllerState.activeBrokers.size() == 3
    }, "waiting for broker lease to expire")

    val resultFuture = activeController.createTopic("topic1", 2, 2)
    Await.ready(resultFuture, 5.seconds)

    val entries = activeController.consensus.readEntries(0)
    val records = entries.map(entry => Record.deserialize(new ByteArrayInputStream(entry.data)))
    assert(records(0) == BrokerRecord(0, InetAddressAndPort(address, brokerPorts(0)), 2000))
    assert(records(1) == BrokerRecord(1, InetAddressAndPort(address, brokerPorts(1)), 2000))
    assert(records(2) == BrokerRecord(2, InetAddressAndPort(address, brokerPorts(2)), 2000))
    assert(records(3) == TopicRecord("topic1", ""))
    assert(records(4).isInstanceOf[PartitionRecord])
    assert(records(5).isInstanceOf[PartitionRecord])
  }

}
