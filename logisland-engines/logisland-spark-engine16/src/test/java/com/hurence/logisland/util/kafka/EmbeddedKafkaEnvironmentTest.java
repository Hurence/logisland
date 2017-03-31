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
package com.hurence.logisland.util.kafka;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.serializer.KryoSerializer;
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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
            final KryoSerializer deserializer = new KryoSerializer(true);


            ByteArrayInputStream bais = new ByteArrayInputStream(iterator.next().message());
            Record deserializedRecord = deserializer.deserialize(bais);
            logger.info(deserializedRecord.toString());
            assertEquals(record, deserializedRecord);
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