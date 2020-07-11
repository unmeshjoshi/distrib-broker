package org.dist.mykafka

import org.I0Itec.zkclient.ZkClient
import org.dist.common.TestZKUtils
import org.dist.simplekafka.util.ZKStringSerializer
import org.dist.simplekafka.util.ZkUtils.Broker

object Server2 extends App {
  val zkConnect: String = TestZKUtils.zookeeperConnect
  var zkClient: ZkClient = null
  val zkConnectionTimeout = 10000
  val zkSessionTimeout = 15000
  zkClient = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
  val myZookeeperClient1 = new MyZookeeperClient(zkClient);
  myZookeeperClient1.registerBroker(Broker(1, "10.10.10.11", 8000))
  waitForever

  private def waitForever = {
    while (true) {
      Thread.sleep(1000)
    }
  }
}
