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


import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Map;
import java.util.Properties;

/**
 * Created by lhubert on 15/04/16.
 *
 * Used for plugin tests
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
    public void publish(EmbeddedKafkaEnvironment context, String path, String topic) throws IOException {

        List<KeyedMessage> messages = new ArrayList<>();

        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + context.getBrokerPort());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);

        final KryoSerializer kryoSerializer = new KryoSerializer(true);
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {

            if (file.isFile()) {

                // parse JSON file and getField Array of config elements
                ObjectMapper mapper = new ObjectMapper();
                List<Map<String,String>> rules = mapper.readValue(file, List.class);

                for (Map<String, String> rule : rules) {

                    // for all in array create the rule as an event..
                    Record record = new StandardRecord(CONFIG_TYPE);

                    for (String k : rule.keySet()) record.setStringField(k, rule.get(k));

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    kryoSerializer.serialize(baos, record);
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
