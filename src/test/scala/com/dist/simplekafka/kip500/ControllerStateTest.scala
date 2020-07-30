package com.dist.simplekafka.kip500

import java.util

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

    val command = BrokerHeartbeat("0")
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    kv.applyEntry(walEntry)

    val session = kv.activeBrokers.get("0")
    assert(session == Lease("0", 1000))
  }

  test("client id should be walentry id if client id is not passed") {
    val walDir = TestUtils.tempDir("sessionstest")
    val kv = new ControllerState(walDir)

    val command = BrokerHeartbeat()
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    val clientId = kv.applyEntry(walEntry)

    assert(s"${entryId}" == clientId)
  }

  test("client id should be the one passed by client in RegisterClient request") {
    val walDir = TestUtils.tempDir("sessionstest")
    val kv = new ControllerState(walDir)

    val command = BrokerHeartbeat("client1")
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    val clientId = kv.applyEntry(walEntry)

    assert("client1" == clientId)
  }
}
