package com.dist.simplekafka

import com.dist.simplekafka.common.TopicAndPartition

case class ProduceRequest(topicAndPartition: TopicAndPartition, key:String, message:String)
