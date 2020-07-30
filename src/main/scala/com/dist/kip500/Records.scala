package com.dist.kip500

import com.dist.simplekafka.network.InetAddressAndPort

case class BrokerRecord(brokerId:Int, brokerEpoch:Int, address:InetAddressAndPort)

case class TopicRecord(name:String, topicId:String, deleting:Boolean = false)

case class PartitionRecord(partitionId:String, topicId:String, replicas:List[Int], leader:Int)


