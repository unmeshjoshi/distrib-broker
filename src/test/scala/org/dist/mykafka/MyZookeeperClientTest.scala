package org.dist.mykafka

import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.BrokerChangeListener
import org.dist.simplekafka.util.ZkUtils.Broker
import org.scalatest.FunSuite

class MyZookeeperClientTest extends ZookeeperTestHarness {

  test("should register brokers with zookeeper") {
    val myZookeeperClient = new MyZookeeperClient(zkClient);
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.11", 8000))

    assert(3 == myZookeeperClient.getAllBrokers().size)
  }

  test("should get notified when broker is registered") {
    val myZookeeperClient = new MyZookeeperClient(zkClient);
    val listener = new MyBrokerChangeListener(myZookeeperClient)
    myZookeeperClient.subscribeBrokerChangeListener(listener);

    myZookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.11", 8000))
    myZookeeperClient.registerBroker(Broker(2, "10.10.10.12", 8000))



    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)
  }
}
