package com.hurence.logisland.utils.kafka;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.serializer.EventKryoSerializer;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by lhubert on 15/04/16.
 *
 * Used for plugin tests
 */
public class DocumentPublisher {

    /**
     * Published all files found in path into topic (content is put in content field)
     * @param context
     * @param path
     * @param topic
     * @throws IOException
     */
    public void publish(EmbeddedKafkaEnvironment context, String path, String topic) throws IOException {

        List<KeyedMessage> messages = new ArrayList<>();

        // read a json file at path and publish to topic
        // TODO
        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + context.getBrokerPort());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);

        final EventKryoSerializer kryoSerializer = new EventKryoSerializer(true);

        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile()) {
                String content = SmallFileUtil.getContent(file);
                Event event = new Event(file.getName());
                event.put("name", "String", file.getName());
                event.put("content", "String", content);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                kryoSerializer.serialize(baos, event);
                KeyedMessage<String, byte[]> data = new KeyedMessage(topic, baos.toByteArray());
                baos.close();
                messages.add(data);
            }
        }

        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        producer.close();
    }
}
