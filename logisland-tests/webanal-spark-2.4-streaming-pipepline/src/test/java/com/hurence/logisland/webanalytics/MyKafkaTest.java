//package com.hurence.logisland.webanalytics;
//
//import com.salesforce.kafka.test.KafkaBroker;
//import com.salesforce.kafka.test.KafkaTestUtils;
//import org.apache.kafka.common.Node;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.RegisterExtension;
//
//import java.util.Collections;
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.TimeUnit;
//import java.util.stream.Collectors;
//
///**
// * Runs smoke tests against a PLAINTEXT enabled cluster.
// * @see AbstractSharedKafkaTestResourceTest for additional test case definitions.
// */
//class MyKafkaTest extends AbstractSharedKafkaTestResourceTest {
//
//    /**
//     * We have a single embedded kafka server that gets started when this test class is initialized.
//     *
//     * It's automatically started before any methods are run.
//     * It's automatically stopped after all of the tests are completed.
//     *
//     * This example we start a cluster with 2 brokers (defaults to a single broker) and configure the brokers to
//     * disable topic auto-creation.
//     *
//     * It must be scoped as 'public static' in order for the appropriate startup/shutdown hooks to be called on the extension.
//     */
//    @RegisterExtension
//    static final MyKafkaTest sharedKafkaTestResource = new MyKafkaTest()
//            // Start a cluster with 2 brokers.
//            .withBrokers(2)
//            // Disable topic auto-creation.
//            .withBrokerProperty("auto.create.topics.enable", "false");
//
//    /**
//     * Example in a multi-broker cluster, how to stop an individual broker and bring it back on-line.
//     */
//    @Test
//    void testBringingBrokerOffLine() throws Exception {
//        // Now we want to test behavior by stopping brokerId 2.
//        final KafkaBroker broker2 = sharedKafkaTestResource
//                .getKafkaBrokers()
//                .getBrokerById(2);
//
//        // Shutdown broker Id 2.
//        broker2.stop();
//
//        // It may take a moment for the broker to cleanly shut down.
//        List<Node> nodes = Collections.emptyList();
//        for (int attempts = 0; attempts <= 5; attempts++) {
//            // Describe the cluster and wait for it to go to 1 broker.
//            nodes = getKafkaTestUtils().describeClusterNodes();
//            if (nodes.size() == 1) {
//                break;
//            }
//            Thread.sleep(1000L);
//        }
//
//        // We should only have 1 node now, and it should not include broker Id 2.
//        Assertions.assertEquals(1, nodes.size());
//        nodes.forEach((node) -> Assertions.assertNotEquals(2, node.id(), "Should not include brokerId 2"));
//
//        // Test your applications behavior when a broker becomes unavailable or leadership changes.
//
//        // Bring the broker back up
//        broker2.start();
//
//        // It may take a while for the broker to successfully rejoin the cluster,
//        // Block until the broker has come on-line and then continue.
//        getKafkaTestUtils()
//                .waitForBrokerToComeOnLine(2, 10, TimeUnit.SECONDS);
//
//        // We should have 2 nodes again.
//        nodes = getKafkaTestUtils().describeClusterNodes();
//        Assertions.assertEquals(2, nodes.size());
//
//        // Collect the brokerIds in the cluster.
//        final Set<Integer> foundBrokerIds = nodes.stream()
//                .map(Node::id)
//                .collect(Collectors.toSet());
//
//        Assertions.assertTrue(foundBrokerIds.contains(1), "Found brokerId 1");
//        Assertions.assertTrue(foundBrokerIds.contains(2), "Found brokerId 2");
//    }
//
//    /**
//     * Simple accessor.
//     */
//    protected KafkaTestUtils getKafkaTestUtils() {
//        return sharedKafkaTestResource.getKafkaTestUtils();
//    }
//
//    @Override
//    protected String getExpectedListenerProtocol() {
//        return "PLAINTEXT";
//    }
//}