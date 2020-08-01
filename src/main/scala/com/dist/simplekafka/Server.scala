package com.dist.simplekafka

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.dist.simplekafka.api.{RequestKeys, RequestOrResponse}
import com.dist.simplekafka.common.{JsonSerDes, Logging}
import com.dist.simplekafka.kip500.{BrokerHeartbeat, FetchRequest, FetchResponse, HeartBeatTask}
import com.dist.simplekafka.network.InetAddressAndPort
import com.dist.simplekafka.server.Config

class Server(val config:Config, val zookeeperClient: ZookeeperClient, val controller:ZkController, val socketServer: SimpleSocketServer, kip500Mode:Boolean = false) extends Logging {

  val executor = new ScheduledThreadPoolExecutor(1);


  def sendHearbeat() = {
    val updateMetadataRequest = BrokerHeartbeat(config.brokerId, InetAddressAndPort.create(config.hostName, config.port))
    val request = RequestOrResponse(com.dist.simplekafka.kip500.election.RequestKeys.BrokerHeartbeat.asInstanceOf[Short], JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet())
    info(s"Sending heartbeat from broker ${config.brokerId}")
    val response = socketServer.sendReceiveTcp(request, config.kip500ControllerAddress)
    info(s"Received heartbeat response in broker ${config.brokerId} ${response.messageBodyJson}")
  }


  var scheduledTasks:ScheduledFuture[_] = _

  def fetchMetadata() = {
    val updateMetadataRequest = FetchRequest(0)
    val request = RequestOrResponse(com.dist.simplekafka.kip500.election.RequestKeys.Fetch.asInstanceOf[Short], JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet())
    info(s"Sending fetch request from broker ${config.brokerId}")
    val response = socketServer.sendReceiveTcp(request, config.kip500ControllerAddress)
    val fetchResponse = JsonSerDes.deserialize(response.messageBodyJson, classOf[FetchResponse])
    info(s"Received fetch response in broker ${config.brokerId} ${fetchResponse}")

  }

  def startup() = {
    socketServer.startup()
    if (kip500Mode) {
      sendHearbeat()
      scheduledTasks = executor.scheduleWithFixedDelay(new FetchMetadataTask(fetchMetadata), 1000, 1000, TimeUnit.MILLISECONDS)
      info(s"Starting hearbeat task ${1000} ms")

    } else {
      zookeeperClient.registerSelf()
      controller.startup()
    }

    info(s"Server ${config.brokerId} started with log dir ${config.logDirs}")
  }

  class FetchMetadataTask(action:()⇒Unit) extends Runnable {
    override def run(): Unit = {
      action()
    }
  }

  def shutdown()= {
    zookeeperClient.shutdown()
    socketServer.shutdown()
  }

  val correlationId = new AtomicInteger(0)

}
