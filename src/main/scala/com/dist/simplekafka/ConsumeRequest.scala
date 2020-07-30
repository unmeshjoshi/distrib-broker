package com.dist.simplekafka

import com.dist.simplekafka.common.TopicAndPartition

case class ConsumeRequest(topicAndPartition: TopicAndPartition, offset:Int = 0, replicaId:Int = -1)
