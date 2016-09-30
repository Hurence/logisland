package com.hurence.logisland.processor;

import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.StandardComponentContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.KryoSerializer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public class QueryMatcherProcessorTest {
/*

    private static Logger logger = LoggerFactory.getLogger(QueryMatcherProcessorTest.class);
    static EmbeddedKafkaEnvironment context;

    static String docspath = "logisland-common-parsers-plugin/src/main/resources/data/documents/frenchpress";
    static String rulespath = "logisland-common-parsers-plugin/src/main/resources/data/rules";

    static String[] arg1 = new String[]{"--topic", "docs", "--partitions", "1", "--replication-factor", "1"};
    static String[] arg2 = new String[]{"--topic", "rules", "--partitions", "1", "--replication-factor", "1"};
    static String[] arg3 = new String[]{"--topic", "matches", "--partitions", "1", "--replication-factor", "1"};


    @BeforeClass
    public static void initEventsAndQueries() throws IOException {


        // create docs input topic
        context.getKafkaUnitServer().createTopic("docs");
        context.getKafkaUnitServer().createTopic("rules");
        context.getKafkaUnitServer().createTopic("matches");

        try {
            // send documents in path dir to topic
            logger.info("start publishing documents to topics");
            DocumentPublisher publisher = new DocumentPublisher();
            publisher.publish(context, docspath, "docs");

            // send the rules to rule topic
            logger.info("start publishing rules to topics");
            RulesPublisher rpublisher = new RulesPublisher();
            rpublisher.publish(context, rulespath, "rules");
        } catch (Exception e) {
            logger.error("Unexpected exception while publishing docs {}", e.getMessage());
        }

        logger.info("done");
    }

    // @Test
    public void testProcess() throws Exception {

        // setup simple consumer for docs
        Properties consumerProperties = TestUtils.createConsumerProperties(context.getZkConnect(), "group0", "consumer0", -1);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

        // reading rules
        KafkaRulesConsumer rconsumer = new KafkaRulesConsumer();
        List<MatchingRule> rules = rconsumer.consume(context, "rules", "rules", "consumer0");
        String rulesAsString = rules.stream()
                .map(MatchingRule::getQuery)
                .collect(Collectors.joining(", "));


        System.out.println("Rules to apply : " + rules.size());

        //init a processor instance and its context

        QueryMatcherProcessor processor = new QueryMatcherProcessor();
        StandardProcessorInstance instance = new StandardProcessorInstance(processor, "0");
        instance.setProperty("rules",rulesAsString);
        ComponentContext context = new StandardComponentContext(instance);
        processor.init(context);



        // starting consumer for docs...
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("docs", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get("docs").get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        System.out.println("Documents to process:" + iterator.size());

        while (iterator.hasNext()) {

            final KryoSerializer deserializer = new KryoSerializer(true);
            ByteArrayInputStream bais = new ByteArrayInputStream(iterator.next().message());
            Record deserializedRecord = deserializer.deserialize(bais);
            ArrayList<Record> list = new ArrayList<>();
            list.add(deserializedRecord);
            Collection<Record> result = processor.process(context, list);
            for (Record e : result) {
                System.out.println(e.getField("name").getRawValue() + " : " + e.getField("matchingrules").getRawValue());
            }

            bais.close();

        }

        // cleanup
        consumer.shutdown();

    }


    @BeforeClass
    public static void setup() throws Exception {
        // create an embedded Kafka Context
        context = new EmbeddedKafkaEnvironment();
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (context != null)
            context.close();
    }*/
}
