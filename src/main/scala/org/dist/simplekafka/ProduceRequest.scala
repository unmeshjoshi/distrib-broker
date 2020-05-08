package org.dist.simplekafka

import org.dist.simplekafka.common.TopicAndPartition

case class ProduceRequest(topicAndPartition: TopicAndPartition, key:String, message:String)
