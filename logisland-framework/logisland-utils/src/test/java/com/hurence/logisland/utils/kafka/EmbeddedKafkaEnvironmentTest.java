package com.hurence.logisland.utils.kafka;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.serializer.EventKryoSerializer;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by lhubert on 15/04/16.
 */
public class EmbeddedKafkaEnvironmentTest {


    private String topic = "test";
    private EmbeddedKafkaEnvironment context = null;

    private static Logger logger = LoggerFactory.getLogger(EmbeddedKafkaEnvironmentTest.class);




    @Test
    public void producerTest() throws InterruptedException, IOException, NoSuchFieldException, IllegalAccessException {

        /**
         * setup an embedded Kafka environment
         * create a topic
         * send a serialized event to it
         */

        // embeded Kafka
        EmbeddedKafkaEnvironment context = new EmbeddedKafkaEnvironment();
        assertTrue(context.getZkClient() != null);
        ZkClient zkClient = context.getZkClient();
        context.getKafkaUnitServer().createTopic(topic);

        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + context.getBrokerPort());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);

        // create an event
        Event event = new Event("cisco");
        event.put("timestamp", "Long", new Date().getTime());
        event.put("method", "String", "GET");
        event.put("ipSource", "String", "123.34.45.123");
        event.put("ipTarget", "String", "178.23.45.234");
        event.put("urlScheme", "String", "http");
        event.put("urlHost", "String", "hurence.com");
        event.put("urlPort", "String", "80");
        event.put("urlPath", "String", "idea/help/create-test.html");
        event.put("requestSize", "Int", 4578);
        event.put("responseSize", "Int", 452);
        event.put("isOutsideOfficeHours", "Boolean", true);
        event.put("isHostBlacklisted", "Boolean", false);
        event.put("tags", "String", "spam,filter,mail");

        // serialize event
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final EventKryoSerializer kryoSerializer = new EventKryoSerializer(true);
        kryoSerializer.serialize(baos, event);
        KeyedMessage<String, byte[]> data = new KeyedMessage(topic, baos.toByteArray());
        baos.close();
        List<KeyedMessage> messages = new ArrayList<>();
        messages.add(data);

        // send event to Kafka topic
        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        producer.close();


        /**
         * start a Kafka consumer
         *
         */
        // deleting zookeeper information to make sure the consumer starts from the beginning
        // see https://stackoverflow.com/questions/14935755/how-to-get-data-from-old-offset-point-in-kafka
        zkClient.delete("/consumers/group0");

        // setup simple consumer
        Properties consumerProperties = TestUtils.createConsumerProperties(context.getZkConnect(), "group0", "consumer0", 500);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
        // starting consumer
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        // verify the integrity of the retrieved event
        if (iterator.hasNext()) {
            final EventKryoSerializer deserializer = new EventKryoSerializer(true);


            ByteArrayInputStream bais = new ByteArrayInputStream(iterator.next().message());
            Event deserializedEvent = deserializer.deserialize(bais);
            logger.info(deserializedEvent.toString());
            assertEquals(event, deserializedEvent);
            bais.close();
        } else {
            fail();
        }

        /**
         * final cleanup
         */
        consumer.shutdown();
        context.close();
    }
}