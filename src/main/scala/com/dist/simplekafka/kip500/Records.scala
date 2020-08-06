package com.dist.simplekafka.kip500

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, InputStream}

import com.dist.simplekafka.common.JsonSerDes
import com.dist.simplekafka.network.InetAddressAndPort

object RecordType {
  val RegisterBroker = 1
  val FenceBroker = 2
  val BrokerRecord = 3
  val TopicRecord = 4
  val PartitionRecord = 5
  //following is just for initial testing of replicated kv
  val SetValue = 6
}


object BrokerState extends Enumeration {
  type BrokerState = Value
  val UNKNOWN, INITIAL, FENCED, ACTIVE, SHUTDOWN = Value
}

object Record {
  def deserialize(is:InputStream): Record = {
    val daos = new DataInputStream(is)
    val commandType = daos.readInt()
    if (commandType == RecordType.SetValue) {
      SetValueRecord.deserialize(daos)
    } else if (commandType == RecordType.RegisterBroker) {
      BrokerRecord.deserialize(daos)
    } else if (commandType == RecordType.FenceBroker) {
      FenceBroker.deserialize(daos)
    } else if (commandType == RecordType.TopicRecord) {
      TopicRecord.deserialize(daos)
    } else if (commandType == RecordType.PartitionRecord) {
      PartitionRecord.deserialize(daos)
    } else throw new IllegalArgumentException(s"Unknown commandType ${commandType}")
  }
}

trait Record {
  def serialize():Array[Byte]
}

object SetValueRecord {
  def deserialize(is:InputStream) = {
    val daos = new DataInputStream(is)
    val key = daos.readUTF()
    val value = daos.readUTF()
    val clientId = daos.readUTF()
    val sequenceNo = daos.readInt()
    SetValueRecord(key, value, clientId, sequenceNo)
  }
}

case class SetValueRecord(val key:String, val value:String, val clientId:String = "", val sequenceNo:Int = 0) extends Record {
  def serialize() = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(RecordType.SetValue)
    dataStream.writeUTF(key)
    dataStream.writeUTF(value)
    dataStream.writeUTF(clientId)
    dataStream.writeInt(sequenceNo)
    baos.toByteArray
  }
}

object BrokerRecord {
  def deserialize(is:InputStream) = {
    val daos = new DataInputStream(is)
    val clientId = daos.readInt()
    val address = JsonSerDes.deserialize(daos.readUTF(), classOf[InetAddressAndPort])
    val ttl = daos.readLong()
    BrokerRecord(clientId, address, ttl)
  }
}

case class BrokerRecord(val brokerId:Int, address:InetAddressAndPort, ttl:Long) extends Record {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(RecordType.RegisterBroker)
    dataStream.writeInt(brokerId)
    dataStream.writeUTF(JsonSerDes.serialize(address))
    dataStream.writeLong(ttl)
    baos.toByteArray
  }
}


case class FenceBroker(val clientId:Int) extends Record {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(RecordType.FenceBroker)
    dataStream.writeInt(clientId)
    baos.toByteArray
  }
}

object FenceBroker {
  def deserialize(is: InputStream): Record = {
    val daos = new DataInputStream(is)
    val clientId = daos.readInt()
    FenceBroker(clientId)
  }
}

object TopicRecord {
  def deserialize(is: InputStream): Record = {
    val daos = new DataInputStream(is)
    val topicName = daos.readUTF()
    val topicId = daos.readUTF()
    val deleting = daos.readBoolean()
    TopicRecord(topicName, topicId, deleting)
  }
}
case class TopicRecord(name:String, topicId:String = "", deleting:Boolean = false) extends Record {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(RecordType.TopicRecord)
    dataStream.writeUTF(name)
    dataStream.writeUTF(topicId)
    dataStream.writeBoolean(deleting)
    baos.toByteArray
  }
}

object PartitionRecord {
  def deserialize(is: InputStream): Record = {
    val daos = new DataInputStream(is)
    val partitionId = daos.readInt()
    val topicId = daos.readUTF()
    val replicas = JsonSerDes.deserialize(daos.readUTF(), classOf[List[Int]])
    val leader = daos.readInt()
    PartitionRecord(partitionId, topicId, replicas, leader)
  }
}

case class PartitionRecord(partitionId:Int, topicId:String, replicas:List[Int], leader:Int) extends Record {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(RecordType.PartitionRecord)
    dataStream.writeInt(partitionId)
    dataStream.writeUTF(topicId)
    dataStream.writeUTF(JsonSerDes.serialize(replicas))
    dataStream.writeInt(leader)
    baos.toByteArray
  }
}

case class CreateTopicRequest(topicName:String, noOfPartitions:Int, replicationFactor:Int)
case class CreateTopicResponse(topicName:String)

