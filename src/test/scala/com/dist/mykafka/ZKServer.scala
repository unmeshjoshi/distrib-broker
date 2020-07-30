package com.dist.mykafka

import akka.actor.ActorSystem
import com.dist.common.{EmbeddedZookeeper, TestZKUtils}
import com.dist.simplekafka.util.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

object ZKServer extends App {
  var zookeeper: EmbeddedZookeeper = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect)
}


