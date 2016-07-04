package com.hurence.logisland.processor;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.serializer.EventKryoSerializer;
import com.hurence.logisland.utils.kafka.EmbeddedKafkaEnvironment;
import com.hurence.logisland.utils.kafka.DocumentPublisher;
import com.hurence.logisland.utils.kafka.RulesPublisher;
import com.hurence.logisland.rules.KafkaRulesConsumer;
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

/**
 * Created by lhubert on 20/04/16.
 */
public class QueryMatcherProcessorTest {


    private static Logger logger = LoggerFactory.getLogger(QueryMatcherProcessorTest.class);
    static EmbeddedKafkaEnvironment context ;

    static String docspath = "./data/documents/frenchpress";
    static String rulespath = "./data/rules";

    static String[] arg1 = new String[] {"--topic", "docs", "--partitions", "1", "--replication-factor", "1"};
    static String[] arg2 = new String[] {"--topic", "rules", "--partitions", "1","--replication-factor", "1" };
    static String[] arg3 = new String[] {"--topic", "matches", "--partitions", "1","--replication-factor", "1" };


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
        }

        catch (Exception e) {
            logger.error("Unexpected exception while publishing docs {}", e.getMessage());
        }

        logger.info("done");
    }
   // @Test
    public void testProcess() throws Exception {

        // setup simple consumer for docs
        Properties consumerProperties = TestUtils.createConsumerProperties( context.getZkConnect(), "group0", "consumer0", -1);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

        // reading rules
        KafkaRulesConsumer rconsumer = new KafkaRulesConsumer();
        List<MatchingRule> rules = rconsumer.consume(context, "rules", "rules", "consumer0");

        System.out.println("Rules to apply : " + rules.size());

        LuwakQueryMatcher matcher = new LuwakQueryMatcher(rules);
        QueryMatcherProcessor processor = new QueryMatcherProcessor(matcher);

        // starting consumer for docs...
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("docs", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get("docs").get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        System.out.println("Documents to process:" + iterator.size());

        while (iterator.hasNext()) {

            final EventKryoSerializer deserializer = new EventKryoSerializer(true);
            ByteArrayInputStream bais = new ByteArrayInputStream(iterator.next().message());
            Event deserializedEvent = deserializer.deserialize(bais);
            ArrayList<Event> list = new ArrayList<Event>();
            list.add(deserializedEvent);
            Collection<Event> result = processor.process(list);
            for (Event e : result) {
                System.out.println((String)e.get("name").getValue() + " : " + (String) e.get("matchingrules").getValue());
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
        if(context != null)
            context.close();
    }
}
