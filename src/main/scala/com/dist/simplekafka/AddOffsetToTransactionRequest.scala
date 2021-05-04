package com.dist.simplekafka

import com.dist.simplekafka.common.TopicAndPartition

case class AddOffsetToTransactionRequest(offsets: Map[TopicAndPartition, Int], groupId:String, transactionalId:String)
