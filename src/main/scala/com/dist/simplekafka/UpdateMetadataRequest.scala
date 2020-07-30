package com.dist.simplekafka

import com.dist.simplekafka.util.ZkUtils.Broker

case class UpdateMetadataRequest(aliveBrokers:List[Broker], leaderReplicas:List[LeaderAndReplicas])
