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
package com.hurence.logisland.engine;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Empty Java class for source jar generation (need to publish on OSS sonatype)
 */
public class HBaseServiceTest {


    private static Logger logger = LoggerFactory.getLogger(HBaseServiceTest.class);
    private static Producer producer;
    private static EmbeddedKafkaEnvironment kafkaContext;
    private static EngineConfiguration engineConfiguration;
    private static ConsumerConnector consumer;

    @Before
    public void setUp() throws InterruptedException, IOException {
        kafkaContext = new EmbeddedKafkaEnvironment();
        Properties properties = TestUtils.getProducerConfig("localhost:" + kafkaContext.getBrokerPort());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        producer = new Producer(producerConfig);
        kafkaContext.getKafkaUnitServer().createTopic(AbstractKafkaRecordStream.DEFAULT_ERRORS_TOPIC().getValue());
        kafkaContext.getKafkaUnitServer().createTopic(AbstractKafkaRecordStream.DEFAULT_EVENTS_TOPIC().getValue());
        kafkaContext.getKafkaUnitServer().createTopic(AbstractKafkaRecordStream.DEFAULT_RAW_TOPIC().getValue());

        // deleting zookeeper information to make sure the consumer starts from the beginning
        // see https://stackoverflow.com/questions/14935755/how-to-get-data-from-old-offset-point-in-kafka
        assertTrue(kafkaContext.getZkClient() != null);
        ZkClient zkClient = kafkaContext.getZkClient();
        zkClient.delete("/consumers/group0");

        // setup simple consumer
        Properties consumerProperties = TestUtils.createConsumerProperties(kafkaContext.getZkConnect(), "group0", "consumer0", 3000);
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
        engineConfiguration = createEngineConfiguration();

/*
        File checkpointDir = new File("checkpoints");
        if (checkpointDir.isDirectory())
            FileUtils.forceDelete(checkpointDir);*/

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
                engine.stop(engineContext);
                System.out.println("done");
            }
        };
        Thread t = new Thread(myRunnable);
        logger.info("starting engine thread {}", t.getId());
        t.start();

        Thread.sleep(6000);
        logger.info("done waiting for engine startup");
    }

    @After
    public void tearDown() throws NoSuchFieldException, IllegalAccessException {
        producer.close();
        consumer.shutdown();
        kafkaContext.close();
    }


    private EngineConfiguration createEngineConfiguration() {
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
        conf.setControllerServiceConfigurations(createControllerServiceConfigurations());

        return conf;
    }


    private List<ControllerServiceConfiguration> createControllerServiceConfigurations() {

        Map<String, String> properties = new HashMap<>();
        properties.put("hadoop.configuration.files", "");
        properties.put("zookeeper.quorum", "localhost");
        properties.put("zookeeper.client.port", "2181");
        properties.put("zookeeper.znode.parent", "/hbase");
        properties.put("phoenix.client.jar.location", "");

        ControllerServiceConfiguration conf = new ControllerServiceConfiguration();
        conf.setComponent("com.hurence.logisland.service.hbase.HBase_1_1_2_ClientService");
        conf.setDocumentation("an HBase service");
        conf.setType(ComponentType.SINK.toString());
        conf.setConfiguration(properties);
        conf.setControllerService("hbase_service");

        return Collections.singletonList(conf);
    }


    private StreamConfiguration createProcessorChainConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST().getName(), "localhost:" + kafkaContext.getBrokerPort());
        properties.put(AbstractKafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM().getName(), kafkaContext.getZkConnect());
        properties.put(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR().getName(), "1");
        properties.put(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS().getName(), "2");
        properties.put(AbstractKafkaRecordStream.INPUT_SERIALIZER().getName(), AbstractKafkaRecordStream.KRYO_SERIALIZER().getValue());
        properties.put(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET().getName(), AbstractKafkaRecordStream.LARGEST_OFFSET().getValue());

        StreamConfiguration conf = new StreamConfiguration();
        conf.setComponent(KafkaRecordStreamParallelProcessing.class.getName());
        conf.setType(ComponentType.STREAM.toString());
        conf.setConfiguration(properties);
        conf.setStream("KafkaStream");
        conf.addProcessorConfiguration(createProcessorConfiguration());

        return conf;
    }


    private ProcessorConfiguration createProcessorConfiguration() {
    /*    Map<String, String> properties = new HashMap<>();
        properties.put(PutHBaseCell.HBASE_CLIENT_SERVICE.getName(), "hbase_service");
        properties.put(PutHBaseCell.BATCH_SIZE.getName(), "100");
        properties.put(PutHBaseCell.COLUMN_FAMILY_DEFAULT.getName(), "cf");
        properties.put(PutHBaseCell.COLUMN_QUALIFIER_DEFAULT.getName(), "cq");
        properties.put(PutHBaseCell.ROW_ID_FIELD.getName(), "hbase_rowkey");
        properties.put(PutHBaseCell.TABLE_NAME_DEFAULT.getName(), "logisland");


        ProcessorConfiguration conf = new ProcessorConfiguration();
        conf.setComponent(PutHBaseCell.class.getName());
        conf.setType(ComponentType.SINK.toString());
        conf.setConfiguration(properties);
        conf.setProcessor("put_hbase");*/

        Map<String, String> properties = new HashMap<>();
        properties.put("hbase.client.service", "hbase_service");
        properties.put("columns.field", "hbase_colums");
        properties.put("row.identifier.field", "hbase_rowkey");
        properties.put("table.name.default", "logisland");


        ProcessorConfiguration conf = new ProcessorConfiguration();
        conf.setComponent("com.hurence.logisland.processor.hbase.FetchHBaseRow");
        conf.setType(ComponentType.SINK.toString());
        conf.setConfiguration(properties);
        conf.setProcessor("fetch_hbase");

        return conf;
    }


    private void sendMessage(String topic, Record record) throws IOException {


        // serialize event
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final KryoSerializer kryoSerializer = new KryoSerializer(true);
        kryoSerializer.serialize(baos, record);
        KeyedMessage<String, byte[]> data = new KeyedMessage(topic, baos.toByteArray());
        baos.close();
        List<KeyedMessage> messages = new ArrayList<>();
        messages.add(data);


        // send event to Kafka topic
        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        logger.info("sent");
    }

    private Collection<Record> readMessages(String topic) {

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
    public void validateIntegration() throws NoSuchFieldException, IllegalAccessException, InterruptedException, IOException {

        Record record = new StandardRecord();

        sendMessage(AbstractKafkaRecordStream.DEFAULT_RAW_TOPIC().getValue(),
                new StandardRecord().setStringField("hbase_rowkey", "id1").setStringField("value", "myField"));
        Thread.sleep(3000);
        Collection<Record> records = readMessages(AbstractKafkaRecordStream.DEFAULT_EVENTS_TOPIC().getValue());

        assertTrue(records.size() != 0);

    }
}
