package com.dist.simplekafka

import com.dist.common.{TestUtils, ZookeeperTestHarness}
import com.dist.simplekafka.server.Config
import com.dist.util.Networks
import org.mockito.Mockito._

class ServerTest extends ZookeeperTestHarness {
  test("should register itself to zookeeper on startup") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val client: ZookeeperClient = mock(classOf[ZookeeperClient])
    val leaderElector: ZkController = mock(classOf[ZkController])
    val socketServer:SimpleSocketServer = mock(classOf[SimpleSocketServer])
    var server = new Server(config, client, leaderElector, socketServer)
    server.startup()
    verify(client, atLeastOnce()).registerSelf()
    verify(socketServer, atLeastOnce()).startup()
  }

  test("should elect controller on startup") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val client: ZookeeperClient = mock(classOf[ZookeeperClient])
    val leaderElector: ZkController = mock(classOf[ZkController])
    val socketServer:SimpleSocketServer = mock(classOf[SimpleSocketServer])
    var server = new Server(config, client, leaderElector, socketServer)
    server.startup()
    verify(client, atLeastOnce()).registerSelf()
    verify(leaderElector, atLeastOnce()).startup()
  }
}
