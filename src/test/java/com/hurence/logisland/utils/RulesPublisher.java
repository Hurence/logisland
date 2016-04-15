package com.hurence.logisland.utils;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.serializer.EventKryoSerializer;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by lhubert on 15/04/16.
 */
public class RulesPublisher implements Publisher {

    public void publish(KafkaContext context, String path, String topic) throws IOException {

        // read a json file at path and publish to topic
        // TODO
        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + context.getPort());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);

        final EventKryoSerializer kryoSerializer = new EventKryoSerializer(true);

        Event event = new Event("rule1");
        event.put("name", "String", "rule1");
        event.put("query", "String", "body:immigrants");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        kryoSerializer.serialize(baos, event);

        KeyedMessage<String, byte[]> data = new KeyedMessage(topic, baos.toByteArray());
        baos.close();

        List<KeyedMessage> messages = new ArrayList<>();
        messages.add(data);

        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        producer.close();
    }
}
