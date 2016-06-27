package com.hurence.logisland.querymatcher;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.serializer.EventKryoSerializer;
import com.hurence.logisland.integration.testutils.EmbeddedKafkaEnvironment;
import com.hurence.logisland.integration.testutils.DocumentPublisher;
import com.hurence.logisland.integration.testutils.RulesPublisher;
import com.hurence.logisland.rules.KafkaRulesConsumer;
import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by lhubert on 20/04/16.
 */
public class QueryMatcherProcessorTest {

    static EmbeddedKafkaEnvironment context ;

    static String docspath = "./data/documents/frenchpress";
    static String rulespath = "./data/rules";

    static String[] arg1 = new String[] {"--topic", "docs", "--partitions", "1", "--replication-factor", "1"};
    static String[] arg2 = new String[] {"--topic", "rules", "--partitions", "1","--replication-factor", "1" };
    static String[] arg3 = new String[] {"--topic", "matches", "--partitions", "1","--replication-factor", "1" };


    @BeforeClass
    public static void initEventsAndQueries() throws IOException {

        // create an embedded Kafka Context
        context = new EmbeddedKafkaEnvironment();

        // create docs input topic
        TopicCommand.createTopic(context.getZkClient(), new TopicCommand.TopicCommandOptions(arg1));
        TopicCommand.createTopic(context.getZkClient(), new TopicCommand.TopicCommandOptions(arg2));
        TopicCommand.createTopic(context.getZkClient(), new TopicCommand.TopicCommandOptions(arg3));

        // wait till all topics are created on all servers
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(context.getServers()), "docs", 0, 5000);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(context.getServers()), "rules", 0, 5000);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(context.getServers()), "matches", 0, 5000);

        try {
            // send documents in path dir to topic
            DocumentPublisher publisher = new DocumentPublisher();
            publisher.publish(context, docspath, "docs");

            // send the rules to rule topic
            RulesPublisher rpublisher = new RulesPublisher();
            rpublisher.publish(context, rulespath, "rules");
        }

        catch (Exception e) {
            // log error
        }

    }
    @Test
    public void testProcess() throws Exception {

        // setup simple consumer for docs
        Properties consumerProperties = TestUtils.createConsumerProperties(context.getZkServer().connectString(), "group0", "consumer0", -1);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

        // reading rules
        KafkaRulesConsumer rconsumer = new KafkaRulesConsumer();
        List<MatchingRule> rules = rconsumer.consume(context, "rules", "rules", "consumer0");

        LuwakQueryMatcher matcher = new LuwakQueryMatcher(rules);
        QueryMatcherProcessor processor = new QueryMatcherProcessor(matcher);

        // starting consumer for docs...
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("docs", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get("docs").get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

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


    @AfterClass
    public static void closeAll() throws IOException {
        context.close();
    }
}
