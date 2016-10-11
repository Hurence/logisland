/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.util.spark

import java.net.InetAddress
import java.util.Collections

import kafka.admin.AdminUtils
import kafka.common.TopicAndPartition
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.spark.streaming.kafka.OffsetRange
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions


/**
  * lazy instanciation of the ZkClient, usefull in spark streaming foreachRDD
  *
  * @param createZKClient
  */
class ZookeeperSink(createZKClient: () => ZkClient) extends Serializable {

    lazy val zkClient = createZKClient()
    private val logger = LoggerFactory.getLogger(classOf[ZookeeperSink])

    /**
      * will retrieve the amount of partitions for a given Kafka topic.
      *
      * @param topic the topic to count partition
      */
    def getNumPartitions(topic: String): Option[Int] = {

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
      * retrieves latest consummed offset ranges
      *
      * @param group  the group name
      * @param topics the topic name
      * @return
      */
    def loadOffsetRangesFromZookeeper(group: String, topics: Set[String]): Map[TopicAndPartition, Long] = {

        topics.flatMap(topic => {
            val partitions = getNumPartitions(topic)
            if (partitions.isDefined) {
                (0 until partitions.get).map(i => {
                    val nodePath = s"/consumers/$group/offsets/$topic/$i"

                    try {
                        zkClient.exists(nodePath) match {
                            case true =>
                                val maybeOffset = Option(zkClient.readData[String](nodePath))
                                maybeOffset.map { offset =>
                                    TopicAndPartition(topic, i) -> new String(offset).toLong
                                }

                            case false =>
                                logger info s"Kafka Direct Stream - From Offset - ZK Node ($nodePath) does NOT exist"
                                None
                        }
                    } catch {
                        case scala.util.control.NonFatal(error) =>
                            logger error s"Kafka Direct Stream - From Offset - ZK Node ($nodePath) - Error: $error"
                            None
                    }
                })
            } else None
        }).filter(_.isDefined)
            .map(node => node.get._1 -> node.get._2)
            .toMap
    }





    /**
      * store in zookeeper the latest processed offsets for a given group
      *
      * @param group   the processing group
      * @param o the latest offest
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
            val now = org.joda.time.DateTime.now.getMillis

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