package com.dist.simplekafka

import com.dist.simplekafka.common.TopicAndPartition

case class EndTransactionRequest(transactionalId: String, producerId: Long, committed:Boolean) {

}

case class WriteTxnMarkersRequest(producerId:Long, topicPartition:TopicAndPartition, transactionalId:String, result:TransactionResult)