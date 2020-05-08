package org.dist.simplekafka

import org.dist.simplekafka.util.ZkUtils.Broker

case class UpdateMetadataRequest(aliveBrokers:List[Broker], leaderReplicas:List[LeaderAndReplicas])
