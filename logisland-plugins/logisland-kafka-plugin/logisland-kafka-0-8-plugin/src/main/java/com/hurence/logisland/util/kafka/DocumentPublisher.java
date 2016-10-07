package com.hurence.logisland.util.kafka;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.serializer.KryoSerializer;
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
     * Published all files found in path into topic (content is setField in content field)
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

        final KryoSerializer kryoSerializer = new KryoSerializer(true);

        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile()) {
                String content = SmallFileUtil.getContent(file);
                Record record = new StandardRecord(file.getName());
                record.setStringField("name", file.getName());
                record.setStringField("content", content);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                kryoSerializer.serialize(baos, record);
                KeyedMessage<String, byte[]> data = new KeyedMessage(topic, baos.toByteArray());
                baos.close();
                messages.add(data);
            }
        }

        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        producer.close();
    }
}
