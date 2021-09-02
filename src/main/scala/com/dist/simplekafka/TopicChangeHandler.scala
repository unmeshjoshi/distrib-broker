package com.dist.simplekafka

import java.util
import com.dist.simplekafka.common.Logging
import org.I0Itec.zkclient.IZkChildListener

import scala.collection.immutable.Set
import scala.jdk.CollectionConverters._

class TopicChangeHandler(zookeeperClient:ZookeeperClient, onTopicChange:(String, Seq[PartitionReplicas]) => Unit) extends IZkChildListener with Logging {
  var allTopics: Set[String] = Set.empty
  override def handleChildChange(parentPath: String, currentChildren: util.List[String]): Unit = {
    val newTopics = currentChildren.asScala.toSet -- allTopics
    val deletedTopics = allTopics -- currentChildren.asScala
    //        val deletedPartitionReplicaAssignment = replicaAssignment.filter(p => deletedTopics.contains(p._1._1))
    allTopics = currentChildren.asScala.toSet
    newTopics.foreach(topicName => {
      val replicas: Seq[PartitionReplicas] = zookeeperClient.getPartitionAssignmentsFor(topicName)

      onTopicChange(topicName, replicas)
    })
  }
}
