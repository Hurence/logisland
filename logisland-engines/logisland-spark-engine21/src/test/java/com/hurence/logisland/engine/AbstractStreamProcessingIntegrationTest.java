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

import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.KryoSerializer;
import com.hurence.logisland.stream.spark.AbstractKafkaRecordStream;
import com.hurence.logisland.util.spark.SparkUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * Abstract class for integration testing
 */
public abstract class AbstractStreamProcessingIntegrationTest {


    protected static final String ZKHOST = "127.0.0.1";
    protected static final String BROKERHOST = "127.0.0.1";
    protected static final int BROKERPORT = choosePorts(2)[0];
    protected static final String INPUT_TOPIC = "mock_in";
    protected static final String OUTPUT_TOPIC = "mock_out";
    protected static final String MAGIC_STRING = "the world is so big";


    private static Logger logger = (Logger)LoggerFactory.getLogger(AbstractStreamProcessingIntegrationTest.class);

    private static KafkaProducer<byte[], byte[]> producer;
    private static KafkaConsumer<byte[], byte[]> consumer;
    private static ProcessingEngine engine;
    private static EngineContext engineContext;
    protected EmbeddedZookeeper zkServer;
    private KafkaServer kafkaServer;
    protected ZkUtils zkUtils;


    /**
     * Choose a number of random available ports
     */
    public static int[] choosePorts(int count) {
        try {
            ServerSocket[] sockets = new ServerSocket[count];
            int[] ports = new int[count];
            for (int i = 0; i < count; i++) {
                sockets[i] = new ServerSocket(0, 0, InetAddress.getByName("0.0.0.0"));
                ports[i] = sockets[i].getLocalPort();
            }
            for (int i = 0; i < count; i++)
                sockets[i].close();
            return ports;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setUp() throws InterruptedException, IOException {

        Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);

        SparkUtils.customizeLogLevels();
        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZKHOST + ":" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
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
        if(!AdminUtils.topicExists(zkUtils, AbstractKafkaRecordStream.DEFAULT_ERRORS_TOPIC().getValue()))
            AdminUtils.createTopic(zkUtils,
                AbstractKafkaRecordStream.DEFAULT_ERRORS_TOPIC().getValue(),
                1,
                1,
                new Properties(),
                RackAwareMode.Disabled$.MODULE$);
        if(!AdminUtils.topicExists(zkUtils, AbstractKafkaRecordStream.DEFAULT_RECORDS_TOPIC().getValue()))
            AdminUtils.createTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_RECORDS_TOPIC().getValue(), 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        if(!AdminUtils.topicExists(zkUtils, AbstractKafkaRecordStream.DEFAULT_RAW_TOPIC().getValue()))
            AdminUtils.createTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_RAW_TOPIC().getValue(), 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        if(!AdminUtils.topicExists(zkUtils, AbstractKafkaRecordStream.DEFAULT_METRICS_TOPIC().getValue()))
            AdminUtils.createTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_METRICS_TOPIC().getValue(), 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);





        // deleting zookeeper information to make sure the consumer starts from the beginning
        zkClient.delete("/consumers/group0");

/*
        File checkpointDir = new File("checkpoints");
        if (checkpointDir.isDirectory())
            FileUtils.forceDelete(checkpointDir);
*/

        Optional<EngineContext> instance = getEngineContext();
        assertTrue(instance.isPresent());
        assertTrue(instance.get().isValid());
        engine = instance.get().getEngine();
        engineContext = instance.get();


        SparkUtils.customizeLogLevels();
        System.setProperty("hadoop.home.dir", "/");

        Runnable testRunnable = () -> {
            engine.start(engineContext);
        };

        Thread t = new Thread(testRunnable);
        logger.info("starting engine thread {}", t.getId());
        t.start();

    }

    @After
    public void tearDown() throws NoSuchFieldException, IllegalAccessException, InterruptedException {

        engine.shutdown(engineContext);
        Thread.sleep(2000);

        if (kafkaServer != null) {
            kafkaServer.shutdown();
            // Remove any persistent data
            CoreUtils.delete(kafkaServer.config().logDirs());
        }

        if (zkUtils != null) {
            if(AdminUtils.topicExists(zkUtils, AbstractKafkaRecordStream.DEFAULT_ERRORS_TOPIC().getValue()))
                AdminUtils.deleteTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_ERRORS_TOPIC().getValue());
            if(AdminUtils.topicExists(zkUtils, AbstractKafkaRecordStream.DEFAULT_RECORDS_TOPIC().getValue()))
                AdminUtils.deleteTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_RECORDS_TOPIC().getValue());
            if(AdminUtils.topicExists(zkUtils, AbstractKafkaRecordStream.DEFAULT_RAW_TOPIC().getValue()))
                AdminUtils.deleteTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_RAW_TOPIC().getValue());
            if(AdminUtils.topicExists(zkUtils, AbstractKafkaRecordStream.DEFAULT_METRICS_TOPIC().getValue()))
                AdminUtils.deleteTopic(zkUtils, AbstractKafkaRecordStream.DEFAULT_METRICS_TOPIC().getValue());
            zkUtils.close();
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }
    }


    abstract Optional<EngineContext> getEngineContext();


    protected static void sendRecord(String topic, Record record) throws IOException {

        // setup producer
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<byte[], byte[]>(producerProps);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final KryoSerializer kryoSerializer = new KryoSerializer(true);
        kryoSerializer.serialize(baos, record);
        ProducerRecord<byte[], byte[]> data = new ProducerRecord<>(topic, null, baos.toByteArray());
        producer.send(data);
        baos.close();

        logger.info("sent record : " + record + " to topic " + topic);
        producer.close();
    }

    protected static List<Record> readRecords(String topic) {


        // setup consumer
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");  // to make sure the consumer starts from the beginning of the topic
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));


        List<Record> outputRecords = new ArrayList<>();

        // starting consumer
        ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);

        // verify the integrity of the retrieved event
        for (ConsumerRecord<byte[], byte[]> record : records) {
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

        consumer.close();

        return outputRecords;
    }


}
