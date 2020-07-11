package org.dist.mykafka

import org.I0Itec.zkclient.ZkClient
import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.util.ZKStringSerializer
import org.dist.simplekafka.util.ZkUtils.Broker

class MyZookeeperClientTest extends ZookeeperTestHarness {

  test("should register brokers with zookeeper") {
    val myZookeeperClient = new MyZookeeperClient(zkClient);
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.11", 8000))

    assert(2 == myZookeeperClient.getAllBrokers().size)
  }

  test("should get notified when broker is registered") {
    val zkCLient1 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient1 = new MyZookeeperClient(zkCLient1);

    val listener = new MyBrokerChangeListener(myZookeeperClient1)
    myZookeeperClient1.subscribeBrokerChangeListener(listener);
    myZookeeperClient1.registerBroker(Broker(0, "10.10.10.10", 8000))

    val zkCLient2 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient2 = new MyZookeeperClient(zkCLient2);
    myZookeeperClient2.registerBroker(Broker(1, "10.10.10.11", 8000))


    val zkCLient3 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient3 = new MyZookeeperClient(zkCLient3);
    myZookeeperClient3.registerBroker(Broker(2, "10.10.10.12", 8000))


    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)


    zkCLient3.close()
    zkCLient2.close()

    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 1
    }, "Waiting for all brokers to get added", 1000)
  }
}
