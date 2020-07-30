package com.dist.consensus

import java.util

import org.scalatest.FunSuite

class KVStoreTest extends FunSuite {
  test("should have values restored after a crash/shutdown") {
    val walDir = TestUtils.tempDir("waltest")
    val kv = new KVStore(walDir)

    kv.put("k1", "v1")
    kv.put("k2", "v2")
    kv.close

    val restartedKv = new KVStore(walDir)
    assert(Some("v2") == restartedKv.get("k2"))
    assert(Some("v1") == restartedKv.get("k1"))
  }

  test("should create new session when client is registered") {
    val walDir = TestUtils.tempDir("sessionstest")
    val kv = new KVStore(walDir)

    val command = RegisterClientCommand()
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    kv.applyEntry(walEntry)

    val session = kv.sessions.get(s"${entryId}")
    assert(session == new ClientSession(walEntry.leaderTime, s"${entryId}", new util.HashMap[Int, String]()))
  }

  test("client id should be walentry id if client id is not passed") {
    val walDir = TestUtils.tempDir("sessionstest")
    val kv = new KVStore(walDir)

    val command = RegisterClientCommand()
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    val clientId = kv.applyEntry(walEntry)

    assert(s"${entryId}" == clientId)
  }

  test("client id should be the one passed by client in RegisterClient request") {
    val walDir = TestUtils.tempDir("sessionstest")
    val kv = new KVStore(walDir)

    val command = RegisterClientCommand("client1")
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    val clientId = kv.applyEntry(walEntry)

    assert("client1" == clientId)
  }

  test("If request is sent with same sequence number from same client the same response should be returned") {
    val walDir = TestUtils.tempDir("sessionstest")
    val kv = new KVStore(walDir)

    val command = RegisterClientCommand("client1")
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    val clientId = kv.applyEntry(walEntry)
    assert("client1" == clientId)

    val setValueCommand = SetValueCommand("key1", "value1", "client1", 1)

    val firstValue = kv.applyEntry(WalEntry(entryId + 1, setValueCommand.serialize()))


    val secondValue = kv.applyEntry(WalEntry(entryId + 2, SetValueCommand("key1", "value2", "client1", 1).serialize()))
    val thirdValue = kv.applyEntry(WalEntry(entryId + 3, SetValueCommand("key1", "value2", "client1", 2).serialize()))

    assert(firstValue == secondValue)
    assert(firstValue != thirdValue)
  }

  test("should expire session based on cluster time") {
    val walDir = TestUtils.tempDir("sessionstest")
    val kv = new KVStore(walDir)

    val command = RegisterClientCommand("client1")
    val entryId = 1
    val walEntry = WalEntry(entryId, command.serialize())
    val clientId = kv.applyEntry(walEntry)

    val command2 = RegisterClientCommand("client2")
    val entryId2 = 2
    val walEntry2 = WalEntry(entryId2, command2.serialize())
    val clientId2 = kv.applyEntry(walEntry2)

    val clusterClock = new ClusterClock(new SystemClock())
    val newEpoch = clusterClock.interpolate()
    clusterClock.newEpoch(newEpoch)

    TestUtils.waitUntilTrue(()=> {
      kv.expireSessions(clusterClock.leaderStamp())
      kv.sessions.size() == 0
    }, "waiting for sessions to be expired", 1000, 100)


    assert(kv.sessions.size() == 0)
  }

}
