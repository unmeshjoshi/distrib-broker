package com.dist.simplekafka.kip500

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, InputStream}

object CommandType {
  val RegisterBroker = 1
  val SetValue = 2
}

object Command {
  def deserialize(is:InputStream): Command = {
    val daos = new DataInputStream(is)
    val commandType = daos.readInt()
    if (commandType == CommandType.SetValue) {
      SetValueCommand.deserialize(daos)
    } else if (commandType == CommandType.RegisterBroker) {
      BrokerHeartbeat.deserialize(daos)
    } else throw new IllegalArgumentException(s"Unknown commandType ${commandType}")
  }
}

trait Command {
  def serialize():Array[Byte]
}

object SetValueCommand {
  def deserialize(is:InputStream) = {
    val daos = new DataInputStream(is)
    val key = daos.readUTF()
    val value = daos.readUTF()
    val clientId = daos.readUTF()
    val sequenceNo = daos.readInt()
    SetValueCommand(key, value, clientId, sequenceNo)
  }
}

case class SetValueCommand(val key:String, val value:String, val clientId:String = "", val sequenceNo:Int = 0) extends Command {
  def serialize() = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(CommandType.SetValue)
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

case class BrokerHeartbeat(val brokerId:String = "") extends Command {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(CommandType.RegisterBroker)
    dataStream.writeUTF(brokerId)
    baos.toByteArray
  }
}


case class FenceBroker(val clientId:String = "") extends Command {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(CommandType.RegisterBroker)
    dataStream.writeUTF(clientId)
    baos.toByteArray
  }
}

