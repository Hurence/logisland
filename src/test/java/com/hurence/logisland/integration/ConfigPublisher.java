package com.hurence.logisland.integration;


import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Map;
import java.util.Properties;

/**
 * Created by lhubert on 15/04/16.
 */
public class ConfigPublisher implements Publisher {

    private static String CONFIG_TYPE = "CONFIG_EVENT";


    /**
     * Publishes all JSON objects found in a JSON array in topic. JSON is read from file.
     * @param context
     * @param path
     * @param topic
     * @throws IOException
     */
    public void publish(KafkaContext context, String path, String topic) throws IOException {

        List<KeyedMessage> messages = new ArrayList<>();

        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + context.getPort());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);

        final EventKryoSerializer kryoSerializer = new EventKryoSerializer(true);
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {

            if (file.isFile()) {

                // parse JSON file and get Array of config elements
                ObjectMapper mapper = new ObjectMapper();
                List<Map<String,String>> rules = mapper.readValue(file, List.class);

                for (Map<String, String> rule : rules) {

                    // for all in array create the rule as an event..
                    Event event = new Event(CONFIG_TYPE);

                    for (String k : rule.keySet()) event.put(k, "String", rule.get(k));

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    kryoSerializer.serialize(baos, event);
                    KeyedMessage<String, byte[]> data = new KeyedMessage(topic, baos.toByteArray());
                    baos.close();
                    messages.add(data);

                }
            }
        }


        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        producer.close();
    }
}
