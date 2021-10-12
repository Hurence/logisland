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
//import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
//import com.salesforce.kafka.test.listeners.PlainListener;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.streaming.StreamingQuery;
//import org.apache.spark.sql.streaming.StreamingQueryException;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.RegisterExtension;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.Serializable;
//import java.util.concurrent.Future;
//
//public class WebanalSessionnalizationStructedStreamTestBACKUP implements Serializable {
//
//    private static Logger logger = LoggerFactory.getLogger(WebanalSessionnalizationStructedStreamTestBACKUP.class);
//
//    /**
//     * We have a single embedded Kafka server that gets started when this test class is initialized.
//     *
//     * It's automatically started before any methods are run via the @RegisterExtension annotation.
//     * It's automatically stopped after all of the tests are completed via the @RegisterExtension annotation.
//     */
//    @RegisterExtension
//    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
//        .withBrokerProperty("auto.create.topics.enable", "false")
//        .withBrokerProperty("message.max.bytes", "512000")
//        .registerListener(new PlainListener().onPorts(9092));
//
////    @BeforeAll
////    public static final Create
////
//    /**
//     */
//    @Test
//    public void localTest() throws StreamingQueryException, InterruptedException {
//        final String topicName = "test-simple";
//        sharedKafkaTestResource.getKafkaTestUtils().createTopic(topicName, 1, (short)1);
//
//        SparkSession spark = SparkSession
//                .builder()
//                .master("local[2]")
//                .appName("StructuredSessionizationFromKafka")
//                .getOrCreate();
//        logger.info("Created SparkSession");
//
//    // Subscribe to 1 topic
//        Dataset<Row> df = spark
//                .readStream()//TODO can be batch (read)
//                .format("kafka")
//                .option("kafka.bootstrap.servers", sharedKafkaTestResource.getKafkaConnectString())
//                .option("subscribe", topicName)
//                .option("startingOffsets", "earliest")//latest by default for stream
//                //failOnDataLoss => may be false alarm
//                //kafkaConsumer.pollTimeoutMs
//                //fetchOffset.numRetries
//                //fetchOffset.retryIntervalMs
//                //maxOffsetsPerTrigger
//                .load();
//
//        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
//        //TODO need to have a Elasticsearch Sink ?????
//        //TODO sinon solution de secour (workaround) on écrit dans un autre topic et un autre stream lis ce topic et put dans ES depuis un proc
//
//        logger.info("Adding some events in topic");
//        addingEventsToTopicPartition(topicName, 0, "key1", "value1");
//        addingEventsToTopicPartition(topicName, 0, "key2", "value1");
//        addingEventsToTopicPartition(topicName, 0, "key3", "value1");
//        addingEventsToTopicPartition(topicName, 0, "key1", "value2");
//        addingEventsToTopicPartition(topicName, 0, "key2", "value2");
//        addingEventsToTopicPartition(topicName, 0, "key3", "value2");
//
//        logger.info("Start streaming");
//        // Start running the query that prints the session updates to the console
//        StreamingQuery query = df
//                .writeStream()
//                .outputMode("update")
//                .format("console")
//                .start();
//        query.awaitTermination();
//    }
//
//    private void addingEventsToTopicPartition(String topicName, int partitionId, String key, String value) throws InterruptedException {
//        // Define the record we want to produce
//        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, partitionId, key, value);
//
//        // Create a new producer
//        try (final KafkaProducer<String, String> producer =
//                     sharedKafkaTestResource.getKafkaTestUtils().getKafkaProducer(StringSerializer.class, StringSerializer.class)) {
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
////        try (final KafkaConsumer<String, String> kafkaConsumer =
////                     sharedKafkaTestResource.getKafkaTestUtils().getKafkaConsumer(StringDeserializer.class, StringDeserializer.class)) {
////
////            final List<TopicPartition> topicPartitionList = new ArrayList<>();
////            for (final PartitionInfo partitionInfo: kafkaConsumer.partitionsFor(topicName)) {
////                topicPartitionList.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
////            }
////            kafkaConsumer.assign(topicPartitionList);
////            kafkaConsumer.seekToBeginning(topicPartitionList);
////
////            // Pull records from kafka, keep polling until we get nothing back
////            ConsumerRecords<String, String> records;
////            do {
////                records = kafkaConsumer.poll(2000L);
////                logger.info("Found {} records in kafka", records.count());
////                for (ConsumerRecord<String, String> record: records) {
////                    // Validate
////                    Assertions.assertEquals(key, record.key(), "Key matches expected");
////                    Assertions.assertEquals(value, record.value(), "value matches expected");
////                }
////            }
////            while (!records.isEmpty());
////        }
//    }
////    /**
////     * User-defined data type representing the raw lines with timestamps.
////     */
////    public static class LineWithTimestamp implements Serializable {
////        private String line;
////        private Timestamp timestamp;
////
////        public Timestamp getTimestamp() { return timestamp; }
////        public void setTimestamp(Timestamp timestamp) { this.timestamp = timestamp; }
////
////        public String getLine() { return line; }
////        public void setLine(String sessionId) { this.line = sessionId; }
////    }
////
////    /**
////     * User-defined data type representing the input events
////     */
////    public static class Event implements Serializable {
////        private String sessionId;
////        private Timestamp timestamp;
////
////        public Event() { }
////        public Event(String sessionId, Timestamp timestamp) {
////            this.sessionId = sessionId;
////            this.timestamp = timestamp;
////        }
////
////        public Timestamp getTimestamp() { return timestamp; }
////        public void setTimestamp(Timestamp timestamp) { this.timestamp = timestamp; }
////
////        public String getSessionId() { return sessionId; }
////        public void setSessionId(String sessionId) { this.sessionId = sessionId; }
////    }
////
////    /**
////     * User-defined data type for storing a session information as state in mapGroupsWithState.
////     */
////    public static class SessionInfo implements Serializable {
////        private int numEvents = 0;
////        private long startTimestampMs = -1;
////        private long endTimestampMs = -1;
////
////        public int getNumEvents() { return numEvents; }
////        public void setNumEvents(int numEvents) { this.numEvents = numEvents; }
////
////        public long getStartTimestampMs() { return startTimestampMs; }
////        public void setStartTimestampMs(long startTimestampMs) {
////            this.startTimestampMs = startTimestampMs;
////        }
////
////        public long getEndTimestampMs() { return endTimestampMs; }
////        public void setEndTimestampMs(long endTimestampMs) { this.endTimestampMs = endTimestampMs; }
////
////        public long calculateDuration() { return endTimestampMs - startTimestampMs; }
////
////        @Override public String toString() {
////            return "SessionInfo(numEvents = " + numEvents +
////                    ", timestamps = " + startTimestampMs + " to " + endTimestampMs + ")";
////        }
////    }
////
////    /**
////     * User-defined data type representing the update information returned by mapGroupsWithState.
////     */
////    public static class SessionUpdate implements Serializable {
////        private String id;
////        private long durationMs;
////        private int numEvents;
////        private boolean expired;
////
////        public SessionUpdate() { }
////
////        public SessionUpdate(String id, long durationMs, int numEvents, boolean expired) {
////            this.id = id;
////            this.durationMs = durationMs;
////            this.numEvents = numEvents;
////            this.expired = expired;
////        }
////
////        public String getId() { return id; }
////        public void setId(String id) { this.id = id; }
////
////        public long getDurationMs() { return durationMs; }
////        public void setDurationMs(long durationMs) { this.durationMs = durationMs; }
////
////        public int getNumEvents() { return numEvents; }
////        public void setNumEvents(int numEvents) { this.numEvents = numEvents; }
////
////        public boolean isExpired() { return expired; }
////        public void setExpired(boolean expired) { this.expired = expired; }
////    }
//}