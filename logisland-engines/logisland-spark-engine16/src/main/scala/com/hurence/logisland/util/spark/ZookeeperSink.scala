/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
  * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.hurence.logisland.util.spark

import java.net.InetAddress
import java.util.{Collections, Date}

import kafka.admin.AdminUtils
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.spark.streaming.kafka.OffsetRange
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer


/**
  * lazy instanciation of the ZkClient, usefull in spark streaming foreachRDD
  *
  * @param createZKClient
  */
class ZookeeperSink(createZKClient: () => ZkClient) extends Serializable {

    lazy val zkClient = createZKClient()
    private val logger = LoggerFactory.getLogger(classOf[ZookeeperSink])


    def shutdown() ={
        zkClient.close()
    }

    /**
      * will retrieve the amount of partitions for a given Kafka topic.
      *
      * @param topic the topic to count partition
      */
    def getNumPartitions(brokerList: String, topic: String): Option[Int] = {

        try {
            zkClient.setZkSerializer(new ZkSerializer() {
                @throws[ZkMarshallingError]
                def serialize(o: Object): Array[Byte] = ZKStringSerializer.serialize(o)

                def deserialize(bytes: Array[Byte]): Object = ZKStringSerializer.deserialize(bytes)
            })
            val topicMetadatas = AdminUtils.fetchTopicMetadataFromZk(JavaConversions.asScalaSet(Collections.singleton(topic)), zkClient)
            if (topicMetadatas != null && topicMetadatas.nonEmpty)
                Some(JavaConversions.setAsJavaSet(topicMetadatas).iterator.next.partitionsMetadata.size)
            else {
                logger.info("Failed to get metadata for topic " + topic)
                None
            }

        }
        catch {
            case e: Exception => {
                logger.info("Failed to get metadata for topic " + topic)
                None
            }
        }
    }

    /**
      * retrieve the latest offsets for a given kafka topic
      * it will find offsets for all partitions
      *
      * @param topic
      * @param time timestamp of the offsets before that "timestamp/-1(latest)/-2(earliest)"
      * @return
      */
    def getLatestOffset(brokerList: String, clientId: String, topic: String, time: Long): Map[Int, Option[Long]] = {


        val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)

        val nOffsets = 1
        val maxWaitMs = 1000

        val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, clientId, maxWaitMs).topicsMetadata
        if (topicsMetadata.size != 1 || !topicsMetadata.head.topic.equals(topic)) {
            logger.info(("no valid topic metadata for topic: %s, " + " probably the topic does not exist ").format(topic))
        }

        val partitions = topicsMetadata.head.partitionsMetadata.map(_.partitionId)

        val buffer = new ArrayBuffer[String]()

        partitions.map { partitionId =>
            val partitionMetadataOpt = topicsMetadata.head.partitionsMetadata.find(_.partitionId == partitionId)
            partitionMetadataOpt match {
                case Some(metadata) =>
                    metadata.leader match {
                        case Some(leader) =>
                            try{
                                val consumer = new kafka.consumer.SimpleConsumer(leader.host, leader.port, 10000, 100000, clientId)
                                val topicAndPartition = TopicAndPartition(topic, partitionId)
                                val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(time, nOffsets)))
                                val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets
                                partitionId -> Some(offsets.head)
                            }catch {
                                case ex:Throwable =>
                                    logger.warn("unable to get offsets for partition %d".format(partitionId))
                                    partitionId -> None
                            }

                        case None =>
                            logger.info("partition %d does not have a leader. Skip getting offsets".format(partitionId))
                            partitionId -> None
                    }
                case None =>
                    logger.info("partition %d does not exist".format(partitionId))
                    partitionId -> None
            }
        }.toMap

    }

    /**
      * retrieves latest consumed offset ranges for each partitions of each topic.
      * return latest offset if something went wrong
      *
      * @param group  the group name
      * @param topics the topic names
      * @return
      */
    def loadOffsetRangesFromZookeeper(brokerList: String, group: String, topics: Set[String]): Map[TopicAndPartition, Long] = {
        logger.info(s"loading latest Offsets from Zookeeper, brokerList: $brokerList, consumer group : $group in topics $topics")
        topics.flatMap(topic => {

            // first retrieve latest offsets of th topics
            getLatestOffset(brokerList, group, topic, -1).map( topicOffset => {
                val partitionId = topicOffset._1
                val latestOffset = topicOffset._2
                val zkNodePath = s"/consumers/$group/offsets/$topic/$partitionId"

                if (latestOffset.isEmpty) {
                    logger.info(s"latest offset doesn't exist for partition $partitionId in topic $topic")
                    TopicAndPartition(topic, partitionId) -> -1L
                } else {
                    val latestStoredOffset =
                        try {
                            zkClient.exists(zkNodePath) match {
                                case true =>
                                    val maybeOffset = Option(zkClient.readData[String](zkNodePath))
                                    maybeOffset.map { offset =>
                                        TopicAndPartition(topic, partitionId) -> new String(offset).toLong
                                    }
                                case false =>
                                    logger.info(s"ZK Node ($zkNodePath) does NOT exist")
                                    None
                            }
                        } catch {
                            case scala.util.control.NonFatal(error) =>
                                logger.info(s"ZK Node ($zkNodePath) - Error: $error")
                                None
                        }
                    if (latestStoredOffset.isEmpty || (latestOffset.get < latestStoredOffset.get._2)) {
                        TopicAndPartition(topic, partitionId) -> latestOffset.get
                    } else {
                        TopicAndPartition(topic, partitionId) -> latestStoredOffset.get._2
                    }
                }
            })
        }).toMap
    }


    /**
      * store in zookeeper the latest processed offsets for a given group
      *
      * @param group the processing group
      * @param o     the latest offest
      */
    def saveOffsetRangesToZookeeper(group: String, o: OffsetRange): Unit = {


        // Consumer Offset
        locally {
            val nodePath = s"/consumers/$group/offsets/${o.topic}/${o.partition}"

            if (!zkClient.exists(nodePath))
                zkClient.createPersistent(nodePath, true)
            zkClient.writeData(nodePath, o.untilOffset.toString)
        }


        val hostname = InetAddress.getLocalHost.getHostName
        val ownerId = s"$group-$hostname-${o.partition}"
        val now = new Date().getTime


        // Consumer Ids
        locally {
            val nodePath = s"/consumers/$group/ids/$ownerId"
            val value = s"""{"version":1,"subscription":{"${o.topic}":${o.partition},"pattern":"white_list","timestamp":"$now"}"""

            if (!zkClient.exists(nodePath))
                zkClient.createPersistent(nodePath, true)

            zkClient.writeData(nodePath, value)
        }

        // Consumer Owners
        locally {
            val nodePath = s"/consumers/$group/owners/${o.topic}/${o.partition}"
            val value = ownerId

            if (!zkClient.exists(nodePath))
                zkClient.createPersistent(nodePath, true)

            zkClient.writeData(nodePath, value)
        }
    }

}

object ZookeeperSink {
    private val logger = LoggerFactory.getLogger(classOf[ZookeeperSink])

    def apply(zkQuorum: String): ZookeeperSink = {
        val f = () => {
            logger.info("creating Zk client")
            val zkClient = new ZkClient(zkQuorum, 3000, 3000, ZKStringSerializer)

            zkClient
        }
        new ZookeeperSink(f)
    }
}