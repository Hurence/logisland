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
package com.hurence.logisland.util.kafka


import kafka.admin.ConsumerGroupCommand.LogEndOffsetResult
import kafka.api.{OffsetFetchRequest, OffsetFetchResponse, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.{TopicAndPartition, _}
import kafka.consumer.SimpleConsumer
import kafka.utils._
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.BrokerNotAvailableException
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.security.JaasUtils
import scala.collection.{Set, mutable}

class ConsumerGroupUtils(val zkUrl: String) {

    private val zkUtils = {
        ZkUtils(zkUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled)
    }


    def close() {
        zkUtils.close()
    }

    def getPartitionOffsets(group: String,
                                    topicPartitions: Seq[TopicAndPartition],
                                    channelSocketTimeoutMs: Int,
                                    channelRetryBackoffMs: Int): Map[TopicAndPartition, Long] = {
        val offsetMap = mutable.Map[TopicAndPartition, Long]()
        val channel = ClientUtils.channelToOffsetManager(group, zkUtils, channelSocketTimeoutMs, channelRetryBackoffMs)
        channel.send(OffsetFetchRequest(group, topicPartitions))
        val offsetFetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload())

        offsetFetchResponse.requestInfo.foreach { case (topicAndPartition, offsetAndMetadata) =>
            if (offsetAndMetadata == OffsetMetadataAndError.NoOffset) {
                val topicDirs = new ZKGroupTopicDirs(group, topicAndPartition.topic)
                // this group may not have migrated off zookeeper for offsets storage (we don't expose the dual-commit option in this tool
                // (meaning the lag may be off until all the consumers in the group have the same setting for offsets storage)
                try {
                    val offset = zkUtils.readData(topicDirs.consumerOffsetDir + "/" + topicAndPartition.partition)._1.toLong
                    offsetMap.put(topicAndPartition, offset)
                } catch {
                    case z: ZkNoNodeException =>
                        println("Could not fetch offset from zookeeper for group %s partition %s due to missing offset data in zookeeper."
                            .format(group, topicAndPartition))
                }
            }
            else if (offsetAndMetadata.error == Errors.NONE.code)
                offsetMap.put(topicAndPartition, offsetAndMetadata.offset)
            else
                println("Could not fetch offset from kafka for group %s partition %s due to %s."
                    .format(group, topicAndPartition, Errors.forCode(offsetAndMetadata.error).exception))
        }
        channel.disconnect()
        offsetMap.toMap
    }

    private def getZkConsumer(brokerId: Int): Option[SimpleConsumer] = {
        try {
            zkUtils.getBrokerInfo(brokerId)
                .map(_.getBrokerEndPoint(SecurityProtocol.PLAINTEXT))
                .map(endPoint => new SimpleConsumer(endPoint.host, endPoint.port, 10000, 100000, "ConsumerGroupCommand"))
                .orElse(throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId)))
        } catch {
            case t: Throwable =>
                println("Could not parse broker info due to " + t.getMessage)
                None
        }
    }

    def getLogEndOffset(topic: String, partition: Int): LogEndOffsetResult = {
        zkUtils.getLeaderForPartition(topic, partition) match {
            case Some(-1) => LogEndOffsetResult.Unknown
            case Some(brokerId) =>
                getZkConsumer(brokerId).map { consumer =>
                    val topicAndPartition = new TopicAndPartition(topic, partition)
                    val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
                    val logEndOffset = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
                    consumer.close()
                    LogEndOffsetResult.LogEndOffset(logEndOffset)
                }.getOrElse(LogEndOffsetResult.Ignore)
            case None =>
                println(s"No broker for partition ${new TopicPartition(topic, partition)}")
                LogEndOffsetResult.Ignore
        }
    }
}
