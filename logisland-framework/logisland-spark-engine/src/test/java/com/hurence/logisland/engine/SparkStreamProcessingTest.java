package com.hurence.logisland.engine;

import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.ComponentFactory;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorChainConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.engine.spark.SparkStreamProcessingEngine;
import com.hurence.logisland.processor.MockProcessor;
import com.hurence.logisland.processor.chain.KafkaRecordStream;
import com.hurence.logisland.serializer.KryoSerializer;
import com.hurence.logisland.utils.kafka.EmbeddedKafkaEnvironment;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.elasticsearch.index.engine.Engine;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * Empty Java class for source jar generation (need to publish on OSS sonatype)
 */
public class SparkStreamProcessingTest {



    EngineConfiguration createEngineConfiguration() {
        Map<String,String> properties = new HashMap<>();
        properties.put(SparkStreamProcessingEngine.SPARK_APP_NAME().getName(), "testApp");
        properties.put(SparkStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION().getName(), "1000");
        properties.put(SparkStreamProcessingEngine.SPARK_MASTER().getName(), "local[4]");
        properties.put(SparkStreamProcessingEngine.SPARK_STREAMING_TIMEOUT().getName(), "10000");


        EngineConfiguration conf = new EngineConfiguration();
        conf.setComponent(SparkStreamProcessingEngine.class.getName());
        conf.setType(ComponentType.ENGINE.toString());
        conf.setConfiguration(properties);
        conf.addProcessorChainConfigurations(createProcessorChainConfiguration());

        return conf;
    }


    ProcessorChainConfiguration createProcessorChainConfiguration() {
        Map<String,String> properties = new HashMap<>();
        properties.put(KafkaRecordStream.KAFKA_METADATA_BROKER_LIST.getName(), "localhost:9001");
        properties.put(KafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM.getName(), "localhost:9000");
        properties.put(KafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR.getName(), "1");
        properties.put(KafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS.getName(), "2");

        ProcessorChainConfiguration conf = new ProcessorChainConfiguration();
        conf.setComponent(KafkaRecordStream.class.getName());
        conf.setType(ComponentType.PROCESSOR_CHAIN.toString());
        conf.setConfiguration(properties);
        conf.setProcessorChain("KafkaStream");
        conf.addProcessorConfiguration(createProcessorConfiguration());

        return conf;
    }

    ProcessorConfiguration createProcessorConfiguration() {
        Map<String,String> properties = new HashMap<>();
        properties.put(MockProcessor.FAKE_MESSAGE.getName(), "the world is so big");

        ProcessorConfiguration conf = new ProcessorConfiguration();
        conf.setComponent(MockProcessor.class.getName());
        conf.setType(ComponentType.PROCESSOR.toString());
        conf.setConfiguration(properties);
        conf.setProcessor("mock");

        return conf;
    }


    void sendMessage(String topic, String message,Producer producer){
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

    @Test
    public void validateIntegration() throws NoSuchFieldException, IllegalAccessException, InterruptedException {

        EmbeddedKafkaEnvironment kafkaContext = new EmbeddedKafkaEnvironment();
        EngineConfiguration engineConfiguration = createEngineConfiguration();
        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:9001");

        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);

        Optional<StandardEngineInstance> instance = ComponentFactory.getEngineInstance(engineConfiguration);
        assertTrue(instance.isPresent());
        assertTrue(instance.get().isValid());
        StreamProcessingEngine engine = instance.get().getEngine();
        EngineContext engineContext = new StandardEngineContext(instance.get());



        Runnable myRunnable = new Runnable() {
            @Override
            public void run() {
                System.setProperty("hadoop.home.dir", "/");
                engine.start(engineContext);
                System.out.println("done");
            }
        };
        Thread t = new Thread(myRunnable);
       // logger.info("STARTING THREAD {}", t.getId());
        t.start();


        Runnable myRunnable2 = new Runnable() {
            @Override
            public void run() {
                // todo send send
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                sendMessage("logisland_raw", "ok right now", producer);
            }
        };
        Thread t2 = new Thread(myRunnable2);
        // logger.info("STARTING THREAD {}", t.getId());
        t2.start();



        Thread.sleep(10000);
        producer.close();
        kafkaContext.close();
    }
}
