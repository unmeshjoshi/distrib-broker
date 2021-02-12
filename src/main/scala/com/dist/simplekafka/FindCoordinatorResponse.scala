package com.dist.simplekafka

import com.dist.simplekafka.common.TopicAndPartition

case class FindCoordinatorResponse(topicPartition:TopicAndPartition, partitionInfo:PartitionInfo)
