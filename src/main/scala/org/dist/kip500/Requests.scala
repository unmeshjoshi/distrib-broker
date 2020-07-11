package org.dist.kip500

case class FetchRequest(replicaId:Int, topic:FetchableTopic)

case class FetchableTopic(name:String, partition:FetchPartition)

case class FetchPartition(partitionIndex:Int, fetchOffset:Int = 0)


case class FetchResponse(sessionId:Int, topicResponse:FetchableTopicResponse)

case class FetchableTopicResponse(name:String, partition:FetchPartitionResponse)

case class FetchPartitionResponse(partitionIndex:Int, records:Array[Byte])