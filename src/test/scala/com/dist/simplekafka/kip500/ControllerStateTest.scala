package com.dist.simplekafka.kip500

import java.util

import com.dist.simplekafka.network.InetAddressAndPort
import org.scalatest.FunSuite

class ControllerStateTest extends FunSuite {
  test("should have values restored after a crash/shutdown") {
    val walDir = TestUtils.tempDir("waltest")
    val kv = new ControllerState(walDir)

    kv.put("k1", "v1")
    kv.put("k2", "v2")
    kv.close

    val restartedKv = new ControllerState(walDir)
    assert(Some("v2") == restartedKv.get("k2"))
    assert(Some("v1") == restartedKv.get("k1"))
  }

  test("should create new session when client is registered") {
    val walDir = TestUtils.tempDir("sessionstest")
    val kv = new ControllerState(walDir)

    val command = BrokerHeartbeat(0, InetAddressAndPort.create("10.10.10.10", 8080))
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    kv.applyEntry(walEntry)

    val session = kv.activeBrokers.get(0)
    assert(session.getName == 0)
  }

  test("client id should be the one passed by client in RegisterClient request") {
    val walDir = TestUtils.tempDir("sessionstest")
    val kv = new ControllerState(walDir)

    val command = BrokerHeartbeat(1, InetAddressAndPort.create("10.10.10.10", 8080))
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    val clientId = kv.applyEntry(walEntry)

    assert(1 == clientId)
  }
}
