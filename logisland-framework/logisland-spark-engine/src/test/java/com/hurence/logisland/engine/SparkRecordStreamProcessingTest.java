/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.engine;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine;
import com.hurence.logisland.processor.MockProcessor;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.serializer.KryoSerializer;
import com.hurence.logisland.stream.spark.AbstractKafkaRecordStream;
import com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing;
import com.hurence.logisland.util.spark.SparkUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * Empty Java class for source jar generation (need to publish on OSS sonatype)
 */
public class SparkRecordStreamProcessingTest {


    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private static final String TOPIC = "SparkRecordStreamProcessingTest";


    private static Logger logger = LoggerFactory.getLogger(SparkRecordStreamProcessingTest.class);

    private static KafkaProducer<Integer, byte[]> producer;
    private static EngineConfiguration engineConfiguration;
    private static KafkaConsumer<Integer, byte[]> consumer;
    private static KafkaServer kafkaServer;
    private static ZkClient zkClient;
    private static ZkUtils zkUtils;
    private static EmbeddedZookeeper zkServer;

    @BeforeClass
    public static void setUp() throws InterruptedException, IOException {
        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        // create topics
        AdminUtils.createTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_ERRORS_TOPIC().getValue(), 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        AdminUtils.createTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_EVENTS_TOPIC().getValue(), 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        AdminUtils.createTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_RAW_TOPIC().getValue(), 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        AdminUtils.createTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_METRICS_TOPIC().getValue(), 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);


        // setup producer
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<Integer, byte[]>(producerProps);

        // setup consumer
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");  // to make sure the consumer starts from the beginning of the topic
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC));

        // deleting zookeeper information to make sure the consumer starts from the beginning
        zkClient.delete("/consumers/group0");


        engineConfiguration = createEngineConfiguration();


        File checkpointDir = new File("checkpoints");
        if (checkpointDir.isDirectory())
            FileUtils.forceDelete(checkpointDir);

        Optional<EngineContext> instance = ComponentFactory.getEngineContext(engineConfiguration);
        assertTrue(instance.isPresent());
        assertTrue(instance.get().isValid());
        ProcessingEngine engine = instance.get().getEngine();
        EngineContext engineContext = instance.get();

        Thread.sleep(2000);
        Runnable myRunnable = () -> {
            System.setProperty("hadoop.home.dir", "/");
            engine.start(engineContext);
            SparkUtils.customizeLogLevels();
            System.out.println("done");
        };
        Thread t = new Thread(myRunnable);
        logger.info("starting engine thread {}", t.getId());
        t.start();

        Thread.sleep(6000);
        logger.info("done waiting for engine startup");
    }

    @AfterClass
    public static void tearDown() throws NoSuchFieldException, IllegalAccessException {
        producer.close();
        consumer.close();
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }


    static EngineConfiguration createEngineConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(KafkaStreamProcessingEngine.SPARK_APP_NAME().getName(), "testApp");
        properties.put(KafkaStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION().getName(), "2000");
        properties.put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "local[4]");
        properties.put(KafkaStreamProcessingEngine.SPARK_STREAMING_TIMEOUT().getName(), "4000");


        EngineConfiguration conf = new EngineConfiguration();
        conf.setComponent(KafkaStreamProcessingEngine.class.getName());
        conf.setType(ComponentType.ENGINE.toString());
        conf.setConfiguration(properties);
        conf.addProcessorChainConfigurations(createProcessorChainConfiguration());

        return conf;
    }


    static StreamConfiguration createProcessorChainConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST().getName(), BROKERHOST + ":" + BROKERPORT);
        properties.put(AbstractKafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM().getName(), ZKHOST + ":" + zkServer.port());
        properties.put(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR().getName(), "1");
        properties.put(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS().getName(), "1");
        properties.put(AbstractKafkaRecordStream.INPUT_SERIALIZER().getName(), AbstractKafkaRecordStream.NO_SERIALIZER().getValue());
        properties.put(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET().getName(), AbstractKafkaRecordStream.LARGEST_OFFSET().getValue());

        StreamConfiguration conf = new StreamConfiguration();
        conf.setComponent(KafkaRecordStreamParallelProcessing.class.getName());
        conf.setType(ComponentType.STREAM.toString());
        conf.setConfiguration(properties);
        conf.setStream("KafkaStream");
        conf.addProcessorConfiguration(createProcessorConfiguration());

        return conf;
    }

    static ProcessorConfiguration createProcessorConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(MockProcessor.FAKE_MESSAGE.getName(), "the world is so big");

        ProcessorConfiguration conf = new ProcessorConfiguration();
        conf.setComponent(MockProcessor.class.getName());
        conf.setType(ComponentType.PROCESSOR.toString());
        conf.setConfiguration(properties);
        conf.setProcessor("mock");

        return conf;
    }


    private static void sendRecord(String topic, Record record) throws IOException {


        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final KryoSerializer kryoSerializer = new KryoSerializer(true);
        kryoSerializer.serialize(baos, record);
        ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(TOPIC, 0, baos.toByteArray());
        producer.send(data);
        baos.close();

        logger.info("sent record : " + record + " to topic " + topic);
    }

    private static Collection<Record> readRecords(String topic) {

        List<Record> outputRecords = new ArrayList<>();

        // starting consumer
        ConsumerRecords<Integer, byte[]> records = consumer.poll(1000);

        // verify the integrity of the retrieved event
        for (ConsumerRecord<Integer, byte[]> record : records) {
            final KryoSerializer deserializer = new KryoSerializer(true);

            ByteArrayInputStream bais = new ByteArrayInputStream(record.value());
            Record deserializedRecord = deserializer.deserialize(bais);
            logger.info(deserializedRecord.toString());
            outputRecords.add(deserializedRecord);
            try {
                bais.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return outputRecords;
    }

    @Test
    public void validateIntegration() throws NoSuchFieldException, IllegalAccessException, InterruptedException, IOException {

        // send message
        Record record = new StandardRecord("cisco");
        record.setId("firewall_record1");
        record.setField("method", FieldType.STRING, "GET");
        record.setField("ip_source", FieldType.STRING, "123.34.45.123");
        record.setField("ip_target", FieldType.STRING, "255.255.255.255");
        record.setField("url_scheme", FieldType.STRING, "http");
        record.setField("url_host", FieldType.STRING, "origin-www.20minutes.fr");
        record.setField("url_port", FieldType.STRING, "80");
        record.setField("url_path", FieldType.STRING, "/r15lgc-100KB.js");
        record.setField("request_size", FieldType.INT, 1399);
        record.setField("response_size", FieldType.INT, 452);
        record.setField("is_outside_office_hours", FieldType.BOOLEAN, false);
        record.setField("is_host_blacklisted", FieldType.BOOLEAN, false);
        record.setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")));

        sendRecord(TOPIC, record);
        Thread.sleep(2000);
        Collection<Record> records = readRecords(AbstractKafkaRecordStream.DEFAULT_EVENTS_TOPIC().getValue());

        assertTrue(records.size() != 0);

    }
}
