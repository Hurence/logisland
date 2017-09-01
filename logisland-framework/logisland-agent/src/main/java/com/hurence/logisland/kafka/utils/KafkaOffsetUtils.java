package com.hurence.logisland.kafka.utils;


import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;

import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.util.parsing.json.JSON;

import java.io.IOException;
import java.nio.channels.UnresolvedAddressException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class KafkaOffsetUtils {

    private static final int AVG_LINE_SIZE_IN_BYTES = 1000;

    private static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 30 * 1000;
    private static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 30 * 1000;
    private ZkUtils zkUtils;
    private SimpleConsumer consumer = null;
    private Broker leaderBroker;

    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetUtils.class);


    public KafkaOffsetUtils(String zkUrl) {

        this.zkUtils = ZkUtils.apply(
                zkUrl,
                DEFAULT_ZK_SESSION_TIMEOUT_MS,
                DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                JaasUtils.isZkSecurityEnabled());
    }


    public void close() {
        zkUtils.close();
    }

    public List<String> list() {
        return (List<String>) zkUtils.getConsumerGroups();
    }


    public long getLogEndOffset(String topic, int partition) {
        try {
            SimpleConsumer consumer = findLeaderConsumer(topic, partition);
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            Map<TopicAndPartition, PartitionOffsetRequestInfo> infoMap = new HashMap<>();
            infoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));

            OffsetRequest request = new OffsetRequest(infoMap, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
            OffsetResponse response = consumer.getOffsetsBefore(request);

            // Retrieve offsets from response
            long[] offsets = response.hasError() ? null : response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
            if (offsets == null || offsets.length <= 0) {
                short errorCode = response.errorCode(topicAndPartition.topic(), topicAndPartition.partition());

                // If the topic partition doesn't exists, use offset 0 without logging error.
                if (errorCode != ErrorMapping.UnknownTopicOrPartitionCode()) {
                    log.warn("Failed to fetch latest offset for {}. Error: {}. Default offset to 0.",
                            topicAndPartition, errorCode);
                }
                return 0L;
            }


            consumer.close();

            return offsets[0];


        } catch (Exception ex) {
            log.error("unable to retrieve offset {}", new Object[]{ex});
            return -1;
        }
    }

    private SimpleConsumer findLeaderConsumer(String topic, int partition) {
        Option<Object> leaderForPartition = zkUtils.getLeaderForPartition(topic, partition);

        return getZkConsumer((Integer) leaderForPartition.get());
    }


    private SimpleConsumer getZkConsumer(int brokerId) {
        try {
            Option<String> data = zkUtils.readDataMaybeNull(ZkUtils.BrokerIdsPath() + "/" + brokerId)._1;

            Map<String, Object> brokerInfo = (Map<String, Object>) JSON.parseFull(data.get()).get();
            String host = (String) brokerInfo.get("host");
            int port = (int) brokerInfo.get("port");
            return new SimpleConsumer(host, port, 10000, 100000, "ConsumerGroupCommand");

        } catch (Exception ex) {
            System.out.println("Could not parse broker info due to " + ex.getMessage());
            return null;
        }
    }





}
