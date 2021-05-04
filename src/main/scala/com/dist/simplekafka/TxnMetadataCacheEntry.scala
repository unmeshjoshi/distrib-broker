package com.dist.simplekafka

import scala.collection.mutable

case class TxnMetadataCacheEntry(metadataPerTransactionalId:mutable.Map[String, TransactionMetadata] = mutable.Map[String, TransactionMetadata]()) {

}
