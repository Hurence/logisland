package com.hurence.logisland.testutils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Initialises a Kafka context with an embedded zookeeper server
 * and a client... just ready for producing events
 * For online documentation
 * see
 * https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/kafka/utils/TestUtils.scala
 * https://github.com/apache/kafka/blob/0.8.2/core/src/main/scala/kafka/admin/TopicCommand.scala
 * https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/kafka/admin/TopicCommandTest.scala
 */
public class EmbeddedKafkaEnvironment {

    private int brokerId = 0;
    private int port;
    private static String master = "local[6]";
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private KafkaConfig config;
    KafkaServer kafkaServer;
    List<KafkaServer> servers;

    /**
     * Initialises a testing Kafka environment with an EmbeddedZookeeper
     * , a client and a server
     */
    public EmbeddedKafkaEnvironment() {

        // setup Zookeeper
        String zkConnect = TestZKUtils.zookeeperConnect();
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);

        config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);

    }

    /**
     * Returns a zookeeper client
     *
     * @return
     */
    public ZkClient getZkClient() {
        return zkClient;
    }

    /**
     * Returns the list of Kafka servers
     *
     * @return
     */
    public List<KafkaServer> getServers() {
        return servers;
    }

    /**
     * Return the zookeeper server
     * @return
     */
    public EmbeddedZookeeper getZkServer() {
        return zkServer;
    }

    /**
     * Returns the port
     * @return
     */
    public int getPort() {
        return port;
    }

    /**
     * Method to close all elements of EmbeddedKafkaEnvironment
     */
    public void close() {
        for (KafkaServer server : getServers()) server.shutdown();
        getZkClient().close();
        getZkServer().shutdown();
    }

}
