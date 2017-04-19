/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.engine;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine;
import com.hurence.logisland.util.runner.MockProcessor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.KryoSerializer;
import com.hurence.logisland.stream.spark.AbstractKafkaRecordStream;
import com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing;
import com.hurence.logisland.util.kafka.EmbeddedKafkaEnvironment;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Empty Java class for source jar generation (need to publish on OSS sonatype)
 */
public class SparkRecordStreamProcessingTest {


    private static Logger logger = LoggerFactory.getLogger(SparkRecordStreamProcessingTest.class);
    private static Producer producer;
    private static EmbeddedKafkaEnvironment kafkaContext;
    private static EngineConfiguration engineConfiguration;
    private static ConsumerConnector consumer;

    @BeforeClass
    public static void setUp() throws InterruptedException, IOException {
        kafkaContext = new EmbeddedKafkaEnvironment();
        Properties properties = TestUtils.getProducerConfig("localhost:" + kafkaContext.getBrokerPort());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        producer = new Producer(producerConfig);
        kafkaContext.getKafkaUnitServer().createTopic(AbstractKafkaRecordStream.DEFAULT_ERRORS_TOPIC().getValue());
        kafkaContext.getKafkaUnitServer().createTopic(AbstractKafkaRecordStream.DEFAULT_EVENTS_TOPIC().getValue());
        kafkaContext.getKafkaUnitServer().createTopic(AbstractKafkaRecordStream.DEFAULT_RAW_TOPIC().getValue());
        kafkaContext.getKafkaUnitServer().createTopic(AbstractKafkaRecordStream.DEFAULT_METRICS_TOPIC().getValue());

        // deleting zookeeper information to make sure the consumer starts from the beginning
        // see https://stackoverflow.com/questions/14935755/how-to-get-data-from-old-offset-point-in-kafka
        assertTrue(kafkaContext.getZkClient() != null);
        ZkClient zkClient = kafkaContext.getZkClient();
        zkClient.delete("/consumers/group0");

        // setup simple consumer
        Properties consumerProperties = TestUtils.createConsumerProperties(kafkaContext.getZkConnect(), "group0", "consumer0", 1000);
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
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
        Runnable myRunnable = new Runnable() {
            @Override
            public void run() {
                System.setProperty("hadoop.home.dir", "/");
                engine.start(engineContext);
                System.out.println("done");
            }
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
        consumer.shutdown();
        kafkaContext.close();
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
        conf.addPipelineConfigurations(createProcessorChainConfiguration());

        return conf;
    }


    static StreamConfiguration createProcessorChainConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST().getName(), "localhost:9001");
        properties.put(AbstractKafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM().getName(), "localhost:9000");
        properties.put(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR().getName(), "1");
        properties.put(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS().getName(), "2");
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


    static void sendMessage(String topic, String message) {
        // serialize event
      /*  ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final KryoSerializer kryoSerializer = new KryoSerializer(true);
        kryoSerializer.serialize(baos, record);*/
        KeyedMessage<String, byte[]> data = new KeyedMessage<>(topic, message.getBytes()); //new KeyedMessage(topic, baos.toByteArray());
        //  baos.close();
        List<KeyedMessage> messages = new ArrayList<>();
        messages.add(data);

        // send event to Kafka topic
        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        System.out.println("sent");
    }

    static Collection<Record> readMessages(String topic) {

        List<Record> outputRecords = new ArrayList<>();
        // starting consumer
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        // verify the integrity of the retrieved event
        if (iterator.hasNext()) {
            final KryoSerializer deserializer = new KryoSerializer(true);

            ByteArrayInputStream bais = new ByteArrayInputStream(iterator.next().message());
            Record deserializedRecord = deserializer.deserialize(bais);
            logger.info(deserializedRecord.toString());
            outputRecords.add(deserializedRecord);
            try {
                bais.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            fail();
        }
        return outputRecords;
    }

    @Test
    @Ignore
    public void validateIntegration() throws NoSuchFieldException, IllegalAccessException, InterruptedException {


        sendMessage("logisland_raw", "ok right now");
        Thread.sleep(3000);
        Collection<Record> records = readMessages(AbstractKafkaRecordStream.DEFAULT_EVENTS_TOPIC().getValue());

        assertTrue(records.size() != 0);

    }
}
