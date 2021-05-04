package com.dist.simplekafka

case class EndTransactionRequest(transactionalId: String, producerId: String, committed:Boolean) {

}
