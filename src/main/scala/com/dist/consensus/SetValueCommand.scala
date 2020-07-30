package com.dist.consensus

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, InputStream}

object CommandType {
  val OpenSession = 1
  val SetValue = 2
}

object Command {
  def deserialize(is:InputStream): Command = {
    val daos = new DataInputStream(is)
    val commandType = daos.readInt()
    if (commandType == CommandType.SetValue) {
      SetValueCommand.deserialize(daos)
    } else if (commandType == CommandType.OpenSession) {
      RegisterClientCommand.deserialize(daos)
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

object RegisterClientCommand {
  def deserialize(is:InputStream) = {
    val daos = new DataInputStream(is)
    val clientId = daos.readUTF()
    RegisterClientCommand(clientId)
  }
}

case class RegisterClientCommand(val clientId:String = "") extends Command {
  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(baos)
    dataStream.writeInt(CommandType.OpenSession)
    dataStream.writeUTF(clientId)
    baos.toByteArray
  }
}
