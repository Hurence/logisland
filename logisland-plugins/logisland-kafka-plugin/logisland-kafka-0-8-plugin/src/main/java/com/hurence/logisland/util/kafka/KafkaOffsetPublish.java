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
package com.hurence.logisland.util.kafka;


import kafka.api.ConsumerMetadataRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.network.BlockingChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class KafkaOffsetPublish {


    private static Logger logger = LoggerFactory.getLogger(KafkaOffsetPublish.class);

    public void commitOffsets(final String brokerHost,
                              final int brokerPort,
                              final String group,
                              final String clientId,
                              final String topic,
                              final int partition,
                              final long offset) {
        try {

            /**
             * Step 1: Discover and connect to the offset manager for a consumer
             * group by issuing a consumer metadata request to any broker
             */
            BlockingChannel channel = new BlockingChannel(brokerHost, brokerPort,
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    5000 /* read timeout in millis */);
            channel.connect();

            int correlationId = 0;
            final TopicAndPartition partitionToCommit = new TopicAndPartition(topic, partition);
            channel.send(new ConsumerMetadataRequest(group, ConsumerMetadataRequest.CurrentVersion(), correlationId++, clientId));
            ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

            if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
                Broker offsetManager = metadataResponse.coordinator();
                // if the coordinator is different, from the above channel's host then reconnect
                channel.disconnect();
                channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                        BlockingChannel.UseDefaultBufferSize(),
                        BlockingChannel.UseDefaultBufferSize(),
                        5000 /* read timeout in millis */);
                channel.connect();
            } else {
                // retry (after backoff)
            }


            /**
             *  Step 2: Issue the OffsetCommitRequest or OffsetFetchRequest to the offset manager
             */

// How to commit offsets

            long now = System.currentTimeMillis();
            Map<TopicAndPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
            offsets.put(partitionToCommit, new OffsetAndMetadata(offset, "associated metadata", now));

            OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                    group,
                    offsets,
                    correlationId++,
                    clientId,
                    (short) 1 /* version */); // version 1 and above commit to Kafka, version 0 commits to ZooKeeper
            try {
                channel.send(commitRequest.underlying());
                OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer());
                if (commitResponse.hasError()) {
                    for (final Object partitionErrorCode : commitResponse.errors().values()) {
                        if (((int) partitionErrorCode == ErrorMapping.OffsetMetadataTooLargeCode())) {
                            // You must reduce the size of the metadata if you wish to retry
                        } else if ((int) partitionErrorCode == ErrorMapping.NotCoordinatorForConsumerCode() ||
                                (int) partitionErrorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                            channel.disconnect();
                            // Go to step 1 (offset manager has moved) and then retry the commit to the new offset manager
                        } else {
                            // log and retry the commit
                        }
                    }
                }
            } catch (Exception ioe) {
                channel.disconnect();
                // Go to step 1 and then retry the commit
            }

        } catch (Exception ex) {
            // retry the query (after backoff)
            logger.error("exception {}", ex.getMessage());
        }

    }

    public void readOffsets(final String brokerHost,
                            final int brokerPort,
                            final String group,
                            final String clientId,
                            final String topic,
                            final int partition) {
        try {

            /**
             * Step 1: Discover and connect to the offset manager for a consumer
             * group by issuing a consumer metadata request to any broker
             */
            BlockingChannel channel = new BlockingChannel(brokerHost, brokerPort,
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    5000 /* read timeout in millis */);
            channel.connect();

            int correlationId = 0;
            final TopicAndPartition partitionToCommit = new TopicAndPartition(topic, partition);
            channel.send(new ConsumerMetadataRequest(group, ConsumerMetadataRequest.CurrentVersion(), correlationId++, clientId));
            ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

            if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
                Broker offsetManager = metadataResponse.coordinator();
                // if the coordinator is different, from the above channel's host then reconnect
                channel.disconnect();
                channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                        BlockingChannel.UseDefaultBufferSize(),
                        BlockingChannel.UseDefaultBufferSize(),
                        5000 /* read timeout in millis */);
                channel.connect();
            } else {
                // retry (after backoff)
            }

// How to fetch offsets
            final TopicAndPartition partitionToRead = new TopicAndPartition(topic, partition);
            List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
            partitions.add(partitionToRead);
            OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                    group,
                    partitions,
                    (short) 1 /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                    correlationId,
                    clientId);
            try {
                channel.send(fetchRequest.underlying());
                OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer());
                OffsetMetadataAndError result = fetchResponse.offsets().get(partitionToRead);
                short offsetFetchErrorCode = result.error();
                if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
                    channel.disconnect();
                    // Go to step 1 and retry the offset fetch
                } /*else if (errorCode == ErrorMapping.OffsetsLoadInProgress()) {
                    // retry the offset fetch (after backoff)
                }*/ else {
                    long retrievedOffset = result.offset();
                    String retrievedMetadata = result.metadata();
                }
            } catch (Exception e) {
                channel.disconnect();
                // Go to step 1 and then retry offset fetch after backoff
            }
        }catch (Exception ex)
        {
            logger.error("exception {}", ex.getMessage());
        }
    }

}
