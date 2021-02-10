package com.hurence.logisland.webanalytics;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.stream.StreamProperties;
import com.hurence.logisland.stream.spark.structured.provider.KafkaProperties;
import com.hurence.logisland.webanalytics.test.util.ConfJobHelper;
import com.hurence.logisland.webanalytics.test.util.EventsGenerator;
import com.hurence.logisland.webanalytics.util.KafkaUtils;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WebanalSessionnalizationStructedStreamTest {

    private static Logger logger = LoggerFactory.getLogger(WebanalSessionnalizationStructedStreamTest.class);

    final static String logisland_raw = "logisland_raw";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1, true, 2,
            logisland_raw);

    private static KafkaUtils kafkaUtils = new KafkaUtils(embeddedKafka);

    EventsGenerator eventGen = new EventsGenerator("divolte_1");

    /**
     */
    @Test
    @Ignore
    public void myTest() throws IOException, InterruptedException, InitializationException {
        String confFilePath = getClass().getClassLoader().getResource("conf/my-conf-test.yaml").getFile();
        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
        Map<String, String> confKafka = new HashMap<>();
        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
        confJob.modifyControllerServiceConf("kafka_service", confKafka);
        confJob.initJob();
        confJob.startJob();

        long ts = 0L;
        while (true) {
            logger.info("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(logisland_raw, 0, event);
            logger.info("Waiting 5 sec");
            long sleep = 5000L;
            ts += sleep;
            Thread.sleep(sleep);
        }
    }
}