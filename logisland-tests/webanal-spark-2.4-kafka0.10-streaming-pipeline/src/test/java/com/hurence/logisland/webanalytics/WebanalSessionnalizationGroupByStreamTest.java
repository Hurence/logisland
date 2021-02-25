package com.hurence.logisland.webanalytics;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
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

@Ignore
public class WebanalSessionnalizationGroupByStreamTest {

    private static Logger logger = LoggerFactory.getLogger(WebanalSessionnalizationGroupByStreamTest.class);

    final static String logisland_raw = "logisland_raw";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1, true, 2,
            logisland_raw);

    private static KafkaUtils kafkaUtils = new KafkaUtils(embeddedKafka);


    /**
     */
    @Test
    public void startFromlatestOffset() throws IOException, InterruptedException, InitializationException {
        final EventsGenerator eventGen = new EventsGenerator("divolte_1");
        final long numberOfREcordsToPutInKafka = 5000;
        //init kafka
        boolean running = true;
        long ts = 0L;
        long tsInitial = ts;
        int partitionId = 0;
        final long increment = 10000L;
        while (running) {
            logger.trace("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(logisland_raw, partitionId, event);
            ts += increment;
            running = ts != increment * numberOfREcordsToPutInKafka + tsInitial;
//            partitionId = partitionId == 0 ? 1 : 0;
        }

        //start logisland
        String confFilePath = getClass().getClassLoader().getResource("conf/final-webanal-conf.yaml").getFile();
        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
        Map<String, String> confKafka = new HashMap<>();
        confKafka.put(KafkaProperties.INPUT_TOPICS().getName(), logisland_raw);
        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
        confKafka.put(KafkaProperties.KAFKA_STARTING_OFFSETS().getName(), "latest");
        confJob.modifyControllerServiceConf("kafka_service", confKafka);
        Map<String, String> confOpenDistro = new HashMap<>();
        confOpenDistro.put(ElasticsearchClientService.ENABLE_SSL.getName(), "true");
        confJob.modifyControllerServiceConf("opendistro_service", confOpenDistro);
        confJob.initJob();
        confJob.startJob();


        running = true;
        tsInitial = ts;
        while (running) {
            logger.trace("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(logisland_raw, partitionId, event);
            ts += increment;
            running = ts != increment * numberOfREcordsToPutInKafka + tsInitial;
//            partitionId = partitionId == 0 ? 1 : 0;
        }

        confJob.awaitTermination();
        confJob.stopJob();
    }

    /**
     */
    @Test
    public void startFromEarliestOffset() throws IOException, InterruptedException, InitializationException {
        final EventsGenerator eventGen = new EventsGenerator("divolte_1");
        final long numberOfREcordsToPutInKafka = 5000;
        //init kafka
        boolean running = true;
        long ts = 0L;
        long tsInitial = ts;
        int partitionId = 0;
        final long increment = 10000L;
        while (running) {
            logger.trace("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(logisland_raw, partitionId, event);
            ts += increment;
            running = ts != increment * numberOfREcordsToPutInKafka + tsInitial;
//            partitionId = partitionId == 0 ? 1 : 0;
        }

        //start logisland
        String confFilePath = getClass().getClassLoader().getResource("conf/final-webanal-conf.yaml").getFile();
        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
        Map<String, String> confKafka = new HashMap<>();
        confKafka.put(KafkaProperties.INPUT_TOPICS().getName(), logisland_raw);
        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
        confKafka.put(KafkaProperties.KAFKA_STARTING_OFFSETS().getName(), "earliest");
//        confKafka.put(KafkaProperties.KAFKA_STARTING_OFFSETS().getName(), "latest");
        confJob.modifyControllerServiceConf("kafka_service", confKafka);
        Map<String, String> confOpenDistro = new HashMap<>();
        confOpenDistro.put(ElasticsearchClientService.ENABLE_SSL.getName(), "true");
        confJob.modifyControllerServiceConf("opendistro_service", confOpenDistro);
        confJob.initJob();
        confJob.startJob();


        running = true;
        tsInitial = ts;
        while (running) {
            logger.trace("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(logisland_raw, partitionId, event);
            ts += increment;
            running = ts != increment * numberOfREcordsToPutInKafka + tsInitial;
//            partitionId = partitionId == 0 ? 1 : 0;
        }

        confJob.awaitTermination();
        confJob.stopJob();
    }



    /**
     */
    @Test
    public void restartFromLastOffset() throws IOException, InterruptedException, InitializationException {
        final EventsGenerator eventGen = new EventsGenerator("divolte_2");
        final long numberOfREcordsToPutInKafka = 5000;
        //init kafka
        boolean running = true;
        long ts = 0L;
        long tsInitial = ts;
        int partitionId = 0;
        final long increment = 10000L;
        while (running) {
            logger.trace("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(logisland_raw, partitionId, event);
            ts += increment;
            running = ts != increment * numberOfREcordsToPutInKafka + tsInitial;
//            partitionId = partitionId == 0 ? 1 : 0;
        }

        //start logisland
        String confFilePath = getClass().getClassLoader().getResource("conf/final-webanal-conf.yaml").getFile();
        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
        Map<String, String> confKafka = new HashMap<>();
        confKafka.put(KafkaProperties.INPUT_TOPICS().getName(), logisland_raw);
        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
        confKafka.put(KafkaProperties.KAFKA_STARTING_OFFSETS().getName(), "earliest");
        confJob.modifyControllerServiceConf("kafka_service", confKafka);
        Map<String, String> confOpenDistro = new HashMap<>();
        confOpenDistro.put(ElasticsearchClientService.ENABLE_SSL.getName(), "true");
        confJob.modifyControllerServiceConf("opendistro_service", confOpenDistro);
        confJob.initJob();
        confJob.startJob();
        Thread.sleep(30000);
        confJob.stopJob();




        running = true;
        tsInitial = ts;
        while (running) {
            logger.trace("Adding an event in topic");
            Record event = eventGen.generateEvent(ts, "url");
            kafkaUtils.addingEventsToTopicPartition(logisland_raw, partitionId, event);
            ts += increment;
            running = ts != increment * numberOfREcordsToPutInKafka + tsInitial;
//            partitionId = partitionId == 0 ? 1 : 0;
        }

        confJob = new ConfJobHelper(confFilePath);
        confKafka.put(KafkaProperties.KAFKA_STARTING_OFFSETS().getName(), "latest");
        confJob.modifyControllerServiceConf("kafka_service", confKafka);
        confJob.modifyControllerServiceConf("opendistro_service", confOpenDistro);
        confJob.initJob();
        confJob.startJob();

        confJob.awaitTermination();
    }


//    /**
//     */
//    @Test
//    public void showBugWhenSamecheckpoint() throws IOException, InterruptedException, InitializationException {
//        final EventsGenerator eventGen = new EventsGenerator("divolte_1");
//        final long numberOfREcordsToPutInKafka = 5000;
//        //init kafka
//        boolean running = true;
//        long ts = 0L;
//        long tsInitial = ts;
//        int partitionId = 0;
//        final long increment = 10000L;
//        while (running) {
//            logger.trace("Adding an event in topic");
//            Record event = eventGen.generateEvent(ts, "url");
//            kafkaUtils.addingEventsToTopicPartition(logisland_raw, partitionId, event);
//            ts += increment;
//            running = ts != increment * numberOfREcordsToPutInKafka + tsInitial;
////            partitionId = partitionId == 0 ? 1 : 0;
//        }
//
//        //start logisland
//        String confFilePath = getClass().getClassLoader().getResource("conf/final-webanal-conf.yaml").getFile();
//        ConfJobHelper confJob = new ConfJobHelper(confFilePath);
//        Map<String, String> confKafka = new HashMap<>();
//        confKafka.put(KafkaProperties.INPUT_TOPICS().getName(), logisland_raw);
//        confKafka.put(KafkaProperties.KAFKA_ZOOKEEPER_QUORUM().getName(), embeddedKafka.getZookeeperConnectionString());
//        confKafka.put(KafkaProperties.KAFKA_METADATA_BROKER_LIST().getName(), embeddedKafka.getBrokersAsString());
//        confKafka.put(KafkaProperties.KAFKA_STARTING_OFFSETS().getName(), "earliest");
////        confKafka.put(KafkaProperties.KAFKA_STARTING_OFFSETS().getName(), "latest");
//        confJob.modifyControllerServiceConf("kafka_service", confKafka);
//        Map<String, String> confOpenDistro = new HashMap<>();
//        confOpenDistro.put(ElasticsearchClientService.ENABLE_SSL.getName(), "true");
//        confJob.modifyControllerServiceConf("opendistro_service", confOpenDistro);
//        confJob.initJob();
//        confJob.startJob();
//
//
//        running = true;
//        tsInitial = ts;
//        while (running) {
//            logger.trace("Adding an event in topic");
//            Record event = eventGen.generateEvent(ts, "url");
//            kafkaUtils.addingEventsToTopicPartition(logisland_raw, partitionId, event);
//            ts += increment;
//            running = ts != increment * numberOfREcordsToPutInKafka + tsInitial;
////            partitionId = partitionId == 0 ? 1 : 0;
//        }
//
//        confJob.awaitTermination();
//        confJob.stopJob();
//    }
}