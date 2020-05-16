package org.dist.mykafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.simplekafka.common.Logging
import org.dist.simplekafka.util.ZkUtils.Broker

class MyBrokerChangeListener(zookeeperClient:MyZookeeperClient) extends IZkChildListener with Logging {
  var liveBrokers: Set[Broker] = Set()
  import scala.jdk.CollectionConverters._

  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
    info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.asScala.mkString(",")))
    try {

      val curBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
      val newBrokerIds = curBrokerIds -- liveBrokerIds
      val newBrokers = newBrokerIds.map(b => zookeeperClient.getBrokerInfo(b))

      newBrokers.foreach(b => liveBrokers += b)

    } catch {
      case e: Throwable => error("Error while handling broker changes", e)
    }
  }

  private def liveBrokerIds = {
    liveBrokers.map(broker => broker.id)
  }
}