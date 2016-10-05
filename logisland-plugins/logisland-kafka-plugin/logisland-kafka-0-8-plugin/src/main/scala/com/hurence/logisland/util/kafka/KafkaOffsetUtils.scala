/*
 Copyright 2016 Hurence

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.hurence.logisland.util.kafka

import java.util

import _root_.kafka.api.PartitionOffsetRequestInfo
import _root_.kafka.common.TopicAndPartition
import _root_.kafka.javaapi.consumer.SimpleConsumer
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
  * Created by tom on 22/10/15.
  */
object KafkaOffsetUtils extends LazyLogging {


    /**
      * retrieve the latest offset for a given kafka partition
      *
      * @param brokerList
      * @param topic
      * @param partition
      * @param whichTime
      * @return
      */
    def getLastOffset(brokerList: String, topic: String, partition: Int, whichTime: Long): Long = {

        val broker = brokerList.split(",")(0).split(":")
        val host = broker(0)
        val port = broker(1).toInt
        val consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, "leaderLookup")


        val topicAndPartition = new TopicAndPartition(topic, partition)
        val requestInfo = new util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]()
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1))
        val request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, "leaderLookup")
        val response = consumer.getOffsetsBefore(request)

        if (response.hasError) {
            logger.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition))
            0
        } else {
            val offsets = response.offsets(topic, partition)
            offsets(0)
        }

    }
}
