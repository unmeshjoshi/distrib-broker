package com.dist.simplekafka.kip500

import com.dist.simplekafka.network.InetAddressAndPort
import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, InputStream}

import com.dist.simplekafka.common.JsonSerDes

object RecordType {
  val RegisterBroker = 1
  val FenceBroker = 2
  val BrokerRecord = 3
  val TopicRecord = 4
  val PartitionRecord = 5
  //following is just for initial testing of replicated kv
  val SetValue = 6
}

object Record {
  def deserialize(is:InputStream): Record = {
    val daos = new DataInputStream(is)
    val commandType = daos.readInt()
    if (commandType == RecordType.SetValue) {
      SetValueRecord.deserialize(daos)
    } else if (commandType == RecordType.RegisterBroker) {
      BrokerHeartbeat.deserialize(daos)
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

object BrokerHeartbeat {
  def deserialize(is:InputStream) = {
    val daos = new DataInputStream(is)
    val clientId = daos.readUTF()
    BrokerHeartbeat(clientId)
  }
}

case class BrokerHeartbeat(val brokerId:String = "") extends Record {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(RecordType.RegisterBroker)
    dataStream.writeUTF(brokerId)
    baos.toByteArray
  }
}


case class FenceBroker(val clientId:String = "") extends Record {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(RecordType.FenceBroker)
    dataStream.writeUTF(clientId)
    baos.toByteArray
  }
}

object FenceBroker {
  def deserialize(is: InputStream): Record = {
    val daos = new DataInputStream(is)
    val clientId = daos.readUTF()
    FenceBroker(clientId)
  }
}

object BrokerRecord {
  def deserialize(is: InputStream): Record = {
    val daos = new DataInputStream(is)
    val brokerId = daos.readInt()
    val brokerEpoch = daos.readInt()
    val addressJson = daos.readUTF()
    BrokerRecord(brokerId, brokerEpoch, JsonSerDes.deserialize(addressJson, classOf[InetAddressAndPort]))
  }
}

case class BrokerRecord(brokerId:Int, brokerEpoch:Int, address:InetAddressAndPort) extends Record {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(RecordType.BrokerRecord)
    dataStream.writeInt(brokerId)
    dataStream.writeInt(brokerEpoch)
    dataStream.writeUTF(JsonSerDes.serialize(address))
    baos.toByteArray
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
case class TopicRecord(name:String, topicId:String, deleting:Boolean = false) extends Record {
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


