package com.dist.simplekafka.kip500

import java.io.{ByteArrayInputStream, DataInputStream}

import org.scalatest.FunSuite

class PartitionRecordTest extends FunSuite {
  test("should serialize and deserialize partition record") {
    val partitionRecord = PartitionRecord(0, "topic1", List(0, 1, 2), 0)
    val stream = new ByteArrayInputStream(partitionRecord.serialize())
    assert(new DataInputStream(stream).readInt() == RecordType.PartitionRecord)

    val deserializedRecord = PartitionRecord.deserialize(stream)
    assert(deserializedRecord == partitionRecord)
  }
}
