package org.dist.simplekafka

import org.dist.simplekafka.common.TopicAndPartition
import org.dist.simplekafka.util.ZkUtils.Broker

case class PartitionInfo(leader:Broker, allReplicas:List[Broker])

case class LeaderAndReplicas(topicPartition:TopicAndPartition, partitionStateInfo:PartitionInfo)

case class LeaderAndReplicaRequest(leaderReplicas:List[LeaderAndReplicas])
