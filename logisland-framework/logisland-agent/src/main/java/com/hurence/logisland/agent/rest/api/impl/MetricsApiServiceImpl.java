package com.hurence.logisland.agent.rest.api.impl;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hurence.logisland.agent.rest.api.*;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hurence.logisland.agent.rest.api.NotFoundException;

import com.hurence.logisland.kafka.registry.KafkaRegistry;
import com.hurence.logisland.kafka.registry.KafkaRegistryConfig;
import kafka.admin.ConsumerGroupCommand;
import kafka.api.OffsetRequest;
import kafka.common.TopicAndPartition;
import kafka.utils.ZkUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-07-28T16:23:56.034+02:00")
public class MetricsApiServiceImpl extends MetricsApiService {

    private static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 10 * 1000;
    private static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000;
    private static final int DEFAULT_POLLING_TIMEOUT_MS = 5 * 1000;

    private static final int METRICS_CLEAR_MS = 10 * 1000;
    private static final String DEFAULT_GROUP_IP = "LogislandAgent";


    private static Logger logger = LoggerFactory.getLogger(MetricsApiServiceImpl.class);


    private Map<String, String> metrics = new HashMap<>();


    public MetricsApiServiceImpl(KafkaRegistry kafkaRegistry) {
        super(kafkaRegistry);

        ConsumerThread consumerThread = new ConsumerThread(kafkaRegistry, metrics);
        consumerThread.start();
    }


    private static class ConsumerThread extends Thread {
        private KafkaRegistry kafkaRegistry;
        private KafkaConsumer<String, String> kafkaConsumer;
        private Map<String, String> metrics;

        public ConsumerThread(KafkaRegistry kafkaRegistry, Map<String, String> metrics) {
            this.kafkaRegistry = kafkaRegistry;
            this.metrics = metrics;
        }

        @Override
        public void run() {


            TimerTask timerTask = new TimerTask() {
                @Override
                public void run() {
                    metrics.clear();
                }
            };
            Timer timer = new Timer("MetricsClearedTimer");//create a new Timer
            timer.scheduleAtFixedRate(timerTask, METRICS_CLEAR_MS, METRICS_CLEAR_MS);//this line starts the timer at the same time its executed


            Pattern p = Pattern.compile("(.*?)[.](.*?)[.](.*)");
            ObjectMapper objectMapper = new ObjectMapper();
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaRegistry.getConfig().getString(KafkaRegistryConfig.KAFKA_METADATA_BROKER_LIST_CONFIG));
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP_IP);

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(
                    kafkaRegistry.getConfig().getString(KafkaRegistryConfig.KAFKASTORE_TOPIC_METRICS_CONFIG)),
                    new ConsumerRebalanceListener() {
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                            logger.info("{} topic-partitions are revoked from this consumer\n",
                                    Arrays.toString(partitions.toArray()));
                        }

                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            logger.info("{} topic-partitions are assigned to this consumer\n",
                                    Arrays.toString(partitions.toArray()));
                        }
                    });
            //Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(DEFAULT_POLLING_TIMEOUT_MS);
                    for (ConsumerRecord<String, String> record : records) {
                        String jsonValue = record.value();

                        JsonNode jsonNode = objectMapper.readTree(jsonValue);
                        String type = jsonNode.get("type").asText();
                        String value = "";




                        switch (type) {
                            case "counter":
                            case "gauge":
                                value = jsonNode.get("value").asText();
                                break;
                            case "meter":
                            case "histogram":
                            case "timer":
                                value = jsonNode.get("value").get("n").asText();
                                break;
                            default:
                                value = "";
                                break;
                        }


                        Matcher m = p.matcher(record.key());
                        if (m.matches()) {
                            StringBuilder sbuf = new StringBuilder();
                            Formatter fmt = new Formatter(sbuf);

                            int dot1 = record.key().indexOf(".");
                            int dot2 = record.key().indexOf(".", dot1 + 1);
                            String metricName = record.key().substring(dot2 +1 );


                            fmt.format("%s{ app_id=\"%s\", app_handler=\"%s\" } %s", metricName, m.group(1), m.group(2), value);

                            metrics.put(record.key(), sbuf.toString());
                        }



                    }
                }
            } catch (WakeupException ex) {
                System.out.println("Exception caught " + ex.getMessage());
            } catch (JsonParseException e) {
                e.printStackTrace();
            } catch (JsonMappingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }

    private String mapToString(Map<String, String> map) {
        StringBuilder stringBuilder = new StringBuilder();


        SortedSet<String> sortedKeys =new TreeSet<String>(map.keySet());


        for (String key : sortedKeys) {
            if (stringBuilder.length() > 0) {
                stringBuilder.append("\n");
            }
            String value = map.get(key);
            stringBuilder.append(value != null ? value : "");

        }

        return stringBuilder.toString();
    }

    @Override
    public Response getMetrics(SecurityContext securityContext) throws NotFoundException {


        return Response.ok()
                .type("text/plain")
                .entity(mapToString(metrics))
                .build();
    }
}
