package org.dist.mykafka

import akka.actor.ActorSystem
import org.I0Itec.zkclient.ZkClient
import org.dist.common.{EmbeddedZookeeper, TestZKUtils}
import org.dist.simplekafka.util.ZKStringSerializer

object ZKServer extends App {
  var zookeeper: EmbeddedZookeeper = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect)
}


