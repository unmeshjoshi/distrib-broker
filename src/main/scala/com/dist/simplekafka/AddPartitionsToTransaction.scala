package com.dist.simplekafka

import com.dist.simplekafka.common.TopicAndPartition

case class AddPartitionsToTransaction(transactionalId:String,
                                      producerId:String,
                                      partitions:Set[TopicAndPartition])

