package com.dist.mykafka

import com.dist.common.TestUtils
import org.scalatest.FunSuite

class PartitionTest extends FunSuite {

  test("should append messages to file and return offset") {
    val p = new Partition("topic1", 0, TestUtils.tempDir())
    assert(p.logFile.exists())

    val offset = p.append("k1", "m1".getBytes)
    assert(offset == 1)

    val messages = p.read(offset)
    assert(messages.size == 1)
    assert(messages(0) == "m1")
  }

  test("should append append to the end of the file even if read before write") {
    val p = new Partition("topic1", 0, TestUtils.tempDir())
    assert(p.logFile.exists())

    val offset1 = p.append("k1", "m1".getBytes)
    val offset2 = p.append("k2", "m2".getBytes)
    val offset3 = p.append("k3", "m3".getBytes)
    assert(offset3 == 3)

    val messages = p.read(offset2)
    assert(messages.size == 1)
    assert(messages(0) == "m2")

    val offset4 = p.append("k4", "m4".getBytes)
    assert(offset4 == 4)

  }

}
