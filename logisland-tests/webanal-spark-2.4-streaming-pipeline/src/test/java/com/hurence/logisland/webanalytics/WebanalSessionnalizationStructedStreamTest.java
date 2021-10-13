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
//import com.hurence.logisland.record.Record;
//import com.hurence.logisland.stream.StreamProperties;
//import com.hurence.logisland.serializer.KafkaRecordSerializer;
//import com.hurence.logisland.webanalytics.test.util.ConfJobHelper;
//import com.hurence.logisland.webanalytics.test.util.EventsGenerator;
//import com.hurence.logisland.webanalytics.test.util.TestMappings;
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
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.RegisterExtension;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.io.Serializable;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.Future;
//
//public class WebanalSessionnalizationStructedStreamTest implements Serializable {
//
//    private static Logger logger = LoggerFactory.getLogger(WebanalSessionnalizationStructedStreamTest.class);
//
//    EventsGenerator eventGen = new EventsGenerator("divolte_1");
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
//
//    /**
//     */
//    @Test
//    @Disabled
//    public void myTest() throws IOException, InterruptedException {
//        String confFilePath = getClass().getClassLoader().getResource("conf/my-conf-test.yaml").getFile();
//        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
//        Map<String, String> confKafka = new HashMap<>();
//        confKafka.put(StreamProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), sharedKafkaTestResource.getZookeeperConnectString());
//        confKafka.put(StreamProperties.KAFKA_METADATA_BROKER_LIST().getName(), sharedKafkaTestResource.getKafkaConnectString());
//        confJob.modifyControllerServiceConf("kafka_service", confKafka);
//        confJob.initEngineContext();
//        confJob.startJob();
//
//        final String topicName = "test-simple";
//        sharedKafkaTestResource.getKafkaTestUtils().createTopic(topicName, 1, (short)1);
//
//        long ts = 0L;
//        while (true) {
//            logger.info("Adding an event in topic");
//            Record event = eventGen.generateEvent(ts, "url");
//            addingEventsToTopicPartition(topicName, 0, event);
//            logger.info("Waiting 5 sec");
//            long sleep = 5000L;
//            ts += sleep;
//            Thread.sleep(sleep);
//        }
//    }
//
////    /**
////     *
////     */
////    @Test
////    public void testRewind() throws IOException, InterruptedException {
////        String confFilePath = getClass().getClassLoader().getResource("/conf/my-conf-test.yaml").getFile();
////        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
////        Map<String, String> confKafka = new HashMap<>();
////        confKafka.put(StreamProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), sharedKafkaTestResource.getZookeeperConnectString());
////        confJob.modifyControllerServiceConf("kafka_service", confKafka);
////        confJob.initEngineContext();
////        confJob.startJob();
////
////        final String topicName = "logisland-raw";
////        sharedKafkaTestResource.getKafkaTestUtils().createTopic(topicName, 1, (short)1);
////
////        long ts=0l;
////        while (true) {
////            logger.info("Adding an event in topic");
////            Record event = eventGen.generateEvent(ts, "url");
////            addingEventsToTopicPartition(topicName, 0, event);
////            logger.info("Waiting 5 sec");
////            long sleep =5000L;
////            ts += sleep;
////            Thread.sleep(sleep);
////        }
////    }
//
//    /**
//     */
//    @Test
//    @Disabled
//    public void myTest2() throws StreamingQueryException, InterruptedException {
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
//
//        sharedKafkaTestResource.getKafkaTestUtils().createTopic(topicName, 1, (short)1);
//        logger.info("Start streaming");
//        // Start running the query that prints the session updates to the console
//        StreamingQuery query = df
//                .writeStream()
//                .outputMode("update")
//                .format("console")
//                .start();
//
//        long ts = 0L;
//        while (true) {
//            logger.info("Adding an event in topic");
//            Record event = eventGen.generateEvent(ts, "url");
//            addingEventsToTopicPartition(topicName, 0, event);
//            logger.info("Waiting 5 sec");
//            long sleep = 5000L;
//            ts += sleep;
//            Thread.sleep(sleep);
//        }
//
//
////        query.awaitTermination();
//    }
//
//
//
//    private void addingEventsToTopicPartition(String topicName, int partitionId, Record record) throws InterruptedException {
//        // Define the record we want to produce
//        final ProducerRecord<String, Record> producerRecord = new ProducerRecord<String, Record>(
//                topicName,
//                partitionId,
//                record.getField(TestMappings.eventsInternalFields.getTimestampField()).asString(),
//                record
//        );
//
//        // Create a new producer
//        try (final KafkaProducer<String, Record> producer =
//                     sharedKafkaTestResource.getKafkaTestUtils()
//                             .getKafkaProducer(
//                                     StringSerializer.class,
//                                     KafkaRecordSerializer.class)) {
//
//            // Produce it & wait for it to complete.
//            final Future<RecordMetadata> future = producer.send(producerRecord);
//            producer.flush();
//            while (!future.isDone()) {
//                Thread.sleep(500L);
//            }
//            logger.trace("Produce completed:{}", producerRecord);
//        }
//    }
//}