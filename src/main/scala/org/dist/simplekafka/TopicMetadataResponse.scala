package org.dist.simplekafka

import org.dist.simplekafka.common.TopicAndPartition

case class TopicMetadataResponse(topicPartitions:Map[TopicAndPartition, PartitionInfo])
