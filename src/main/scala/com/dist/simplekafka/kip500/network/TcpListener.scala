package com.dist.simplekafka.kip500.network

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import com.dist.simplekafka.kip500.{Logging, Utils}
import com.dist.simplekafka.network.InetAddressAndPort

import scala.concurrent.Future


class SingularUpdateQueue(handler:RequestOrResponse => Future[RequestOrResponse]) extends Thread {
  val workQueue = new ArrayBlockingQueue[(RequestOrResponse, SocketIO[RequestOrResponse])](100)
  @volatile var running = true

  def shutdown() = {
    running = false
  }

  def submitRequest(req: RequestOrResponse, socketIo:SocketIO[RequestOrResponse]): Unit = {
    workQueue.add((req, socketIo))
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  override def run(): Unit = {
    while (running) {
      val (request, socketIo) = workQueue.take()
      val responseFuture:Future[RequestOrResponse] = handler(request)
      responseFuture.onComplete(response => response.map(r=>socketIo.write(r)))
    }
  }

}


class TcpListener(localEp: InetAddressAndPort, handler: RequestOrResponse ⇒ Future[RequestOrResponse]) extends Thread with Logging {
  val isRunning = new AtomicBoolean(true)
  var serverSocket: ServerSocket = null

  def shudown() = {
    isRunning.set(false)
    Utils.swallow(serverSocket.close())
  }

  val workQueue = new SingularUpdateQueue(handler)

  workQueue.start()

  override def run(): Unit = {
    Utils.swallow({
      serverSocket = new ServerSocket()
      serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))
      info(s"Listening on ${localEp}")
      while (true) {
        val socket = serverSocket.accept()
        val socketIo = new SocketIO(socket, classOf[RequestOrResponse])
        socketIo.readHandleWithSocket((request, socket) ⇒ { //Dont close socket after read. It will be closed after write
          workQueue.submitRequest(request, socketIo)
        })
      }
    }
    )
  }
}