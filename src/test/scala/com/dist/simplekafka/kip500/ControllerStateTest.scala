package com.dist.simplekafka.kip500

import com.dist.simplekafka.network.InetAddressAndPort
import org.scalatest.FunSuite

class ControllerStateTest extends FunSuite {

  test("should create new session when client is registered") {
    val walDir = TestUtils.tempDir("sessionstest")
    val kv = new ControllerState()

    val command = BrokerRecord(0, InetAddressAndPort.create("10.10.10.10", 8080), 2000)
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    kv.applyEntry(walEntry)

    val session = kv.activeBrokers.get(0)
    assert(session.getName == 0)
  }

  test("client id should be the one passed by client in RegisterClient request") {
    val kv = new ControllerState()

    val command = BrokerRecord(1, InetAddressAndPort.create("10.10.10.10", 8080), 2000)
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    val response = kv.applyEntry(walEntry)
    response match {
      case brokerRegistration:BrokerRegistrationResponse =>
        assert(brokerRegistration.error == 0)
        assert(brokerRegistration.brokerEpoch == entryId)
    }
  }
}
