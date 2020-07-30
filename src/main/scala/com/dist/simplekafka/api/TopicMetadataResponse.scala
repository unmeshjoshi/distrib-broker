package com.dist.simplekafka.api

import com.dist.simplekafka.common.ErrorMappings
import com.dist.simplekafka.util.ZkUtils.Broker
import com.dist.simplekafka.util.ZkUtils.Broker


case class TopicMetadataResponse(topicsMetadata: Seq[TopicMetadata],
                                 val correlationId: Int)

case class TopicMetadata(topic: String, partitionsMetadata: Seq[PartitionMetadata], errorCode: Short = ErrorMappings.NoError)

case class PartitionMetadata(partitionId: Int,
                             val leader: Option[Broker],
                             replicas: Seq[Broker],
                             isr: Seq[Broker] = Seq.empty,
                             errorCode: Short = ErrorMappings.NoError)
