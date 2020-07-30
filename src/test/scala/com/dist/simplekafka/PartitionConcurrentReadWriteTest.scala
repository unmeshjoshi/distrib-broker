package com.dist.simplekafka

import akka.actor.ActorSystem
import com.dist.common.TestUtils
import com.dist.simplekafka.common.TopicAndPartition
import com.dist.simplekafka.server.Config
import com.dist.util.Networks
import org.scalatest.FunSuite

import scala.util.Random

class PartitionConcurrentReadWriteTest extends FunSuite {
  implicit val partitionActorSystem = ActorSystem("partitionActorSystem")

  test("Concurrent write to partition should be serialized") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))

    val partition = new Partition(config1, TopicAndPartition("topic1", 0))
    (1 to 100).foreach(i ⇒ {
      new Thread(() => {
        partition.append2(s"key${i}", s"message${i}")
      }).run()
    })

    val messages: Seq[Any] = partition.read(0)
    assert(messages.size == 100)
  }

}
