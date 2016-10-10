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

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka.OffsetRange
import org.slf4j.LoggerFactory



class ZookeeperSink(createZKClient: () => ZkClient) extends Serializable {

    lazy val zkClient = createZKClient()
    private val logger = LoggerFactory.getLogger(classOf[ZookeeperSink])

    def storeOffsetRangeToZookeeper(zkQuorum: String, group: String, offsets: Array[OffsetRange]): Unit = {

        offsets.foreach { o =>
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


}

object ZookeeperSink {
    private val logger = LoggerFactory.getLogger(classOf[ZookeeperSink])

    def apply(zkQuorum: String): ZookeeperSink = {
        val f = () => {
            logger.info("creating Zk client")
            val zkClient = new ZkClient(zkQuorum, 3000, 3000, ZKStringSerializer)

         /*   sys.addShutdownHook {
                logger.info("shuting down ZookeeperSink")
                zkClient.close()
            }*/

            zkClient
        }
        new ZookeeperSink(f)
    }
}