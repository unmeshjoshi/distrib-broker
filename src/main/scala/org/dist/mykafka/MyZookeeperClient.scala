package org.dist.mykafka

import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.dist.simplekafka.BrokerChangeListener
import org.dist.simplekafka.common.JsonSerDes
import org.dist.simplekafka.util.ZkUtils.Broker

import scala.jdk.CollectionConverters._

class MyZookeeperClient(zkClient:ZkClient) {
  val BrokerTopicsPath = "/brokers/topics"
  val BrokerIdsPath = "/brokers/ids"
  val ControllerPath = "/controller"
  val ReplicaLeaderElectionPath = "/topics/replica/leader"

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }


  def registerBroker(broker:Broker) = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }


  def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(brokerId => {
      val data: String = zkClient.readData(getBrokerPath(brokerId.toInt))
      JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    }).toSet
  }

  def getBrokerInfo(brokerId: Int): Broker = {
    val data: String = zkClient.readData(getBrokerPath(brokerId))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  private def getBrokerPath(id: Int) = {
    BrokerIdsPath + "/" + id
  }


  def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {
    try {
      client.createPersistent(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }
}
