package com.dist.common

import akka.actor.ActorSystem
import com.dist.simplekafka.util.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.scalatest.{BeforeAndAfterEach, FunSuite}

object TestZKUtils {
  val zookeeperConnect = "127.0.0.1:2182"
}

trait ZookeeperTestHarness extends FunSuite with BeforeAndAfterEach {
  val zkConnect: String = TestZKUtils.zookeeperConnect
  var zookeeper: EmbeddedZookeeper = null
  var zkClient: ZkClient = null
  val zkConnectionTimeout = 10000
  val zkSessionTimeout = 15000
  implicit val partitionActorSystem = ActorSystem("partitionActorSystem")

  override def beforeEach() = {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    zkClient = new ZkClient(zookeeper.connectString, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
  }

  override def afterEach() = {
    zkClient.close()
    zookeeper.shutdown()
  }
}
