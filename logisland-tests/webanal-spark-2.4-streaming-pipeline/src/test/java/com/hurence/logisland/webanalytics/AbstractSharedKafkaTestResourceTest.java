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
//package com.hurence.logisland.webanalytics;
//
//import com.salesforce.kafka.test.KafkaTestUtils;
//import org.apache.kafka.clients.admin.AdminClient;
//import org.apache.kafka.clients.admin.Config;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.apache.kafka.common.Node;
//import org.apache.kafka.common.PartitionInfo;
//import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.config.ConfigResource;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Future;
//import java.util.stream.Collectors;
//
///**
// * Abstract base test class.  This defines shared test cases used by other Concrete tests.
// */
//public abstract class AbstractSharedKafkaTestResourceTest {
//    private static final Logger logger = LoggerFactory.getLogger(AbstractSharedKafkaTestResourceTest.class);
//
//    /**
//     * Validate that we started 2 brokers.
//     */
//    @Test
//    void testTwoBrokersStarted() {
//        final Collection<Node> nodes = getKafkaTestUtils().describeClusterNodes();
//        Assertions.assertNotNull(nodes, "Sanity test, should not be null");
//        Assertions.assertEquals(2, nodes.size(), "Should have two entries");
//
//        // Grab id for each node found.
//        final Set<Integer> foundBrokerIds = nodes.stream()
//                .map(Node::id)
//                .collect(Collectors.toSet());
//
//        Assertions.assertEquals(2, foundBrokerIds.size(), "Found 2 brokers.");
//        Assertions.assertTrue(foundBrokerIds.contains(1), "Found brokerId 1");
//        Assertions.assertTrue(foundBrokerIds.contains(2), "Found brokerId 2");
//    }
//
//    /**
//     * Test consuming and producing via KafkaProducer and KafkaConsumer instances.
//     */
//    @Test
//    void testProducerAndConsumer() throws Exception {
//        // Create a topic
//        final String topicName = "ProducerAndConsumerTest" + System.currentTimeMillis();
//        getKafkaTestUtils().createTopic(topicName, 1, (short) 1);
//
//        final int partitionId = 0;
//
//        // Define our message
//        final String expectedKey = "my-key";
//        final String expectedValue = "my test message";
//
//        // Define the record we want to produce
//        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, partitionId, expectedKey, expectedValue);
//
//        // Create a new producer
//        try (final KafkaProducer<String, String> producer =
//                     getKafkaTestUtils().getKafkaProducer(StringSerializer.class, StringSerializer.class)) {
//
//            // Produce it & wait for it to complete.
//            final Future<RecordMetadata> future = producer.send(producerRecord);
//            producer.flush();
//            while (!future.isDone()) {
//                Thread.sleep(500L);
//            }
//            logger.info("Produce completed");
//        }
//
//        // Create consumer
//        try (final KafkaConsumer<String, String> kafkaConsumer =
//                     getKafkaTestUtils().getKafkaConsumer(StringDeserializer.class, StringDeserializer.class)) {
//
//            final List<TopicPartition> topicPartitionList = new ArrayList<>();
//            for (final PartitionInfo partitionInfo: kafkaConsumer.partitionsFor(topicName)) {
//                topicPartitionList.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
//            }
//            kafkaConsumer.assign(topicPartitionList);
//            kafkaConsumer.seekToBeginning(topicPartitionList);
//
//            // Pull records from kafka, keep polling until we get nothing back
//            ConsumerRecords<String, String> records;
//            do {
//                records = kafkaConsumer.poll(2000L);
//                logger.info("Found {} records in kafka", records.count());
//                for (ConsumerRecord<String, String> record: records) {
//                    // Validate
//                    Assertions.assertEquals(expectedKey, record.key(), "Key matches expected");
//                    Assertions.assertEquals(expectedValue, record.value(), "value matches expected");
//                }
//            }
//            while (!records.isEmpty());
//        }
//    }
//
//    /**
//     * Simple smoke test to ensure broker running appropriate listeners.
//     */
//    @Test
//    void validateListener() throws ExecutionException, InterruptedException {
//        try (final AdminClient adminClient  = getKafkaTestUtils().getAdminClient()) {
//            final ConfigResource broker1Resource = new ConfigResource(ConfigResource.Type.BROKER, "1");
//
//            // Pull broker configs
//            final Config configResult = adminClient
//                    .describeConfigs(Collections.singletonList(broker1Resource))
//                    .values()
//                    .get(broker1Resource)
//                    .get();
//
//            // Check listener
//            final String actualListener = configResult.get("listeners").value();
//            Assertions.assertTrue(
//                    actualListener.contains(getExpectedListenerProtocol() + "://"),
//                    "Expected " + getExpectedListenerProtocol() + ":// and found: " + actualListener);
//
//            // Check inter broker protocol
//            final String actualBrokerProtocol = configResult.get("security.inter.broker.protocol").value();
//            Assertions.assertEquals(getExpectedListenerProtocol(), actualBrokerProtocol, "Unexpected inter-broker protocol");
//        }
//    }
//
//    /**
//     * Simple accessor.
//     */
//    protected abstract KafkaTestUtils getKafkaTestUtils();
//
//    /**
//     * The listener protocol the test is running over.
//     * @return Expected listener protocol
//     */
//    protected abstract String getExpectedListenerProtocol();
//}
//
