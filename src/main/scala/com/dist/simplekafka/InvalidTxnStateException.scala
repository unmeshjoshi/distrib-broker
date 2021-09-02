package com.dist.simplekafka

case class InvalidTxnStateException(str: String) extends RuntimeException(str) {

}
