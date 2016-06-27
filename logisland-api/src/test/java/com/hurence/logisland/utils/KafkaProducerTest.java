package com.hurence.logisland.utils;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.serializer.EventKryoSerializer;
import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * For online documentation
 * see
 * https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/kafka/utils/TestUtils.scala
 * https://github.com/apache/kafka/blob/0.8.2/core/src/main/scala/kafka/admin/TopicCommand.scala
 * https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/kafka/admin/TopicCommandTest.scala
 */
public class KafkaProducerTest {

    private int brokerId = 0;
    private String topic = "test";
    private static String master = "local[6]";

    @Test
    public void producerTest() throws InterruptedException, IOException {

        // setup Zookeeper
        String zkConnect = TestZKUtils.zookeeperConnect();
        EmbeddedZookeeper zkServer = new EmbeddedZookeeper(zkConnect);
        ZkClient zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        int port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        KafkaServer kafkaServer = TestUtils.createServer(config, mock);

        String[] arguments = new String[]{"--topic", topic, "--partitions", "1", "--replication-factor", "1"};
        // create topic
        TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(arguments));

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);

        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + port);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);

        // setup simple consumer
        Properties consumerProperties = TestUtils.createConsumerProperties(zkServer.connectString(), "group0", "consumer0", -1);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

        final EventKryoSerializer kryoSerializer = new EventKryoSerializer(true);

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

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        kryoSerializer.serialize(baos, event);

        KeyedMessage<String, byte[]> data = new KeyedMessage(topic, baos.toByteArray());
        baos.close();

        List<KeyedMessage> messages = new ArrayList<>();
        messages.add(data);

        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        producer.close();

        // deleting zookeeper information to make sure the consumer starts from the beginning
        // see https://stackoverflow.com/questions/14935755/how-to-get-data-from-old-offset-point-in-kafka
        zkClient.delete("/consumers/group0");

        // starting consumer
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        if (iterator.hasNext()) {
            //String msg = new String(iterator.next().message(), StandardCharsets.UTF_8);

            final EventKryoSerializer deserializer = new EventKryoSerializer(true);
            ByteArrayInputStream bais = new ByteArrayInputStream(iterator.next().message());
            Event deserializedEvent = deserializer.deserialize(bais);
            System.out.println(deserializedEvent.toString());
            assertEquals(event, deserializedEvent);
            bais.close();
        } else {
            fail();
        }

        // cleanup
        consumer.shutdown();
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }
}