package com.dist.simplekafka

import com.dist.simplekafka.common.TopicAndPartition

case class OffsetCommitRequest(groupId:String, consumerId:String, offset:Int, topicAndPartition: TopicAndPartition) {

}
