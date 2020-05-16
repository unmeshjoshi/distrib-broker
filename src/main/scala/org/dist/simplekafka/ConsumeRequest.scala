package org.dist.simplekafka

import org.dist.simplekafka.common.TopicAndPartition

case class ConsumeRequest(topicAndPartition: TopicAndPartition, offset:Int = 0, replicaId:Int = -1)
