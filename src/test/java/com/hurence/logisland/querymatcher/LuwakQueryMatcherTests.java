package com.hurence.logisland.querymatcher;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.utils.DocumentPublisher;
import com.hurence.logisland.utils.KafkaContext;
import com.hurence.logisland.utils.RulesPublisher;
import kafka.admin.TopicCommand;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

/**
 * Created by fprunier on 15/04/16.
 */
public class LuwakQueryMatcherTests {

    static KafkaContext context ;

    static String docset = "/documents/frenchpress";
    static String ruleset = "/rules/frenchpress";
    static String docstopic = "docs";
    static String rulestopic = "rules";
    static String matchestopic = "matches";

    static String[] arg1 = new String[] {"--topic", docstopic, "--partitions", "1", "--replication-factor", "1"};
    static String[] arg2 = new String[] {"--topic", rulestopic, "--partitions", "1","--replication-factor", "1" };
    static String[] arg3 = new String[] {"--topic", matchestopic, "--partitions", "1","--replication-factor", "1" };


    @BeforeClass
    public static void initEventsAndQueries() throws IOException {

        // create an embedded Kafka Context
        context = new KafkaContext();

        // create docs input topic
        TopicCommand.createTopic(context.getZkClient(), new TopicCommand.TopicCommandOptions(arg1));
        TopicCommand.createTopic(context.getZkClient(), new TopicCommand.TopicCommandOptions(arg2));
        TopicCommand.createTopic(context.getZkClient(), new TopicCommand.TopicCommandOptions(arg3));

        // wait till all topics are created on all servers
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(context.getServers()), docstopic, 0, 5000);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(context.getServers()), rulestopic, 0, 5000);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(context.getServers()), matchestopic, 0, 5000);

        try {
            // send documents in path dir to topic
            DocumentPublisher publisher = new DocumentPublisher();
            publisher.publish(context, LuwakQueryMatcher.class.getResource(docset).getPath(), docstopic);

            // send the rules to rule topic
            RulesPublisher rpublisher = new RulesPublisher();
            rpublisher.publish(context, LuwakQueryMatcher.class.getResource(docset).getPath(), rulestopic);
        }
        catch (Exception e) {
            // log error
        }

    }

    @Test
    public void testSimpleMatch() {

        MatchingRule rule1 = new MatchingRule("rule1", "name:luke");

        ArrayList<MatchingRule> rules = new ArrayList<>();
        rules.add(rule1);

        LuwakQueryMatcher matcher = new LuwakQueryMatcher(rules);

        Event ev1 = new Event("mytype");
        ev1.put("name","luke");

        Collection<Event> eventsOut = matcher.process(Arrays.asList(ev1));

        assertTrue(eventsOut.size() == 1);
    }

    @AfterClass
    public static void closeAll() throws IOException {
        for (KafkaServer server : context.getServers()) server.shutdown();
        context.getZkClient().close();
        context.getZkServer().shutdown();
    }
}
