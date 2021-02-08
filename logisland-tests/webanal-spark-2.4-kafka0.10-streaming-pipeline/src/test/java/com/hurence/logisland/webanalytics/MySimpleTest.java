package com.hurence.logisland.webanalytics;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.webanalytics.test.util.EventsGenerator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;

public class MySimpleTest {

    private static Logger logger = LoggerFactory.getLogger(MySimpleTest.class);

    final static String topic2 = "topicEvent";
    final static String topic1 = "topic1";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1, true, 2,
            topic1, topic2);

//    /**
//     *
//     */
    @Test
    public void simpleTest2() throws Exception {
        logger.info("Starting test");
        EventsGenerator eventGen = new EventsGenerator("divolte_1");
        logger.info("Adding an event in topic");

        Record event = eventGen.generateEvent(0, "url");
        addingEventsToTopicPartition(topic2, 0, event);
        addingEventsToTopicPartition(topic2, 0, "session1", event);
        addingEventsToTopicPartition(topic2, 0, "session2", event);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        final CountDownLatch latch = new CountDownLatch(3);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<String, Record> kafkaConsumer = new KafkaConsumer<>(consumerProps);
            kafkaConsumer.subscribe(Arrays.asList(topic2, topic1));
            try {
                while (true) {
                    ConsumerRecords<String, Record> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, Record> record : records) {
                        logger.info("consuming from topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        latch.countDown();
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });

        assertTrue(latch.await(90, TimeUnit.SECONDS));
    }


    /**
     *
     */
    @Test
    public void simpleTest() throws Exception {
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);
        producer.send(new ProducerRecord<>(topic1, 0, 0, "message0")).get();
        producer.send(new ProducerRecord<>(topic1, 0, 1, "message1")).get();
        producer.send(new ProducerRecord<>(topic1, 1, 2, "message2")).get();
        producer.send(new ProducerRecord<>(topic1, 1, 3, "message3")).get();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");

        final CountDownLatch latch = new CountDownLatch(4);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
            kafkaConsumer.subscribe(Arrays.asList(topic1, topic2));
            try {
                while (true) {
                    ConsumerRecords<Integer, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<Integer, String> record : records) {
                        logger.info("consuming from topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        latch.countDown();
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });

        assertTrue(latch.await(90, TimeUnit.SECONDS));
    }

    private void addingEventsToTopicPartition(String topicName, int partitionId, Record record) throws InterruptedException {
//        String key = record.getField(TestMappings.eventsInternalFields.getTimestampField()).asString();
        String key = record.getField("ts").asString();
        addingEventsToTopicPartition(topicName, partitionId, key, record);
    }

    private void addingEventsToTopicPartition(String topicName, int partitionId, String key, Record record) throws InterruptedException {
        // Define the record we want to produce
        final ProducerRecord<String, Record> producerRecord = new ProducerRecord<String, Record>(
                topicName,
                partitionId,
                key,
                record
        );

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        senderProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        senderProps.put("value.serializer", "com.hurence.logisland.serializer.KafkaRecordSerializer");
        try (final KafkaProducer<String, Record> producer = new KafkaProducer<>(senderProps)) {
            final Future<RecordMetadata> future = producer.send(producerRecord);
            producer.flush();
            while (!future.isDone()) {
                Thread.sleep(500L);
            }
            logger.trace("Produce completed:{}", producerRecord);
        }  catch (Exception e) {
            logger.error("error while sending data to kafka", e);
        }
    }
}