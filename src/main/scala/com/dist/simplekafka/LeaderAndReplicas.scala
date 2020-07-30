package com.dist.simplekafka

import com.dist.simplekafka.common.TopicAndPartition
import com.dist.simplekafka.util.ZkUtils.Broker

case class PartitionInfo(leader:Broker, allReplicas:List[Broker])

case class LeaderAndReplicas(topicPartition:TopicAndPartition, partitionStateInfo:PartitionInfo)

case class LeaderAndReplicaRequest(leaderReplicas:List[LeaderAndReplicas])
