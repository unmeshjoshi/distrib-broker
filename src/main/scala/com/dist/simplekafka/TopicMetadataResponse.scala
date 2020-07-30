package com.dist.simplekafka

import com.dist.simplekafka.common.TopicAndPartition

case class TopicMetadataResponse(topicPartitions:Map[TopicAndPartition, PartitionInfo])
