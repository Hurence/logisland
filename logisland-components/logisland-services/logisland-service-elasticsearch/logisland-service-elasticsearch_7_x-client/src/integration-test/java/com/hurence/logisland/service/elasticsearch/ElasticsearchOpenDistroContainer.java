/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.service.elasticsearch;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

import java.net.InetSocketAddress;
import java.time.Duration;

import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Represents an elasticsearch docker instance which exposes by default port 9200 and 9300 (transport.tcp.port)
 * The docker image is by default fetched from docker.elastic.co/elasticsearch/elasticsearch
 */
public class ElasticsearchOpenDistroContainer extends GenericContainer<ElasticsearchOpenDistroContainer> {

    /**
     * Elasticsearch Default HTTP port
     */
    private static final int ELASTICSEARCH_OPENDISTRO_DEFAULT_PORT = 9200;

    /**
     * Elasticsearch Default Transport port
     */
    private static final int ELASTICSEARCH_OPENDISTRO_DEFAULT_TCP_PORT = 9300;

    /**
     * Elasticsearch Docker base URL
     */
    private static final String ELASTICSEARCH_OPENDISTRO_DEFAULT_IMAGE = "amazon/opendistro-for-elasticsearch";

    /**
     * Elasticsearch Default version
     */
    protected static final String ELASTICSEARCH_OPENDISTRO_DEFAULT_VERSION = "1.4.0";

    public ElasticsearchOpenDistroContainer() {
        this(ELASTICSEARCH_OPENDISTRO_DEFAULT_IMAGE + ":" + ELASTICSEARCH_OPENDISTRO_DEFAULT_VERSION, null, null);
    }

    /**
     * Create an OpenDistro Elasticsearch Container by passing the full docker image name
     * @param dockerImageName Full docker image name, like: docker.elastic.co/elasticsearch/elasticsearch:6.4.1
     */
    public ElasticsearchOpenDistroContainer(String dockerImageName, String user, String password) {
        super(dockerImageName);

        logger().info("Starting an opendistro elasticsearch container using [{}]", dockerImageName);
        withNetworkAliases("elasticsearch-opendistro-" + Base58.randomString(6));
        withEnv("discovery.type", "single-node");
        // With enforce_hostname_verification enabled, the Security plugin verifies that the hostname of the
        // communication partner matches the hostname in the certificate
//        withEnv("opendistro_security.ssl.transport.enforce_hostname_verification", "false");
        // Do the clients (typically the browser or the proxy) have to authenticate themselves to the http server,
        // default is OPTIONAL. To enforce authentication use REQUIRE, to completely disable client certificates use
        // NONE.
        withEnv("opendistro_security.ssl.http.clientauth_mode", "NONE");
//        withEnv("opendistro_security.ssl.http.enabled", "false"); // Disable https
//        withEnv("opendistro_security.disabled", "true"); // Completely disable security (https; authentication...)
        addExposedPorts(ELASTICSEARCH_OPENDISTRO_DEFAULT_PORT, ELASTICSEARCH_OPENDISTRO_DEFAULT_TCP_PORT);
        HttpWaitStrategy httpWaitStrategy = new HttpWaitStrategy()
                .forPort(ELASTICSEARCH_OPENDISTRO_DEFAULT_PORT)
                .forStatusCodeMatching(response -> response == HTTP_OK)
                .usingTls();

        // Ideally we would like to be able to setup the user with the passed one. For the moment we only support the
        // out of the box admin/admin user
        if ( (user != null) && (password != null) ) {
            httpWaitStrategy.withBasicCredentials(user, password);
        }
        // TODO: if we use the wait strategy then this fails as it not only connects with SSL but it
        // also tries to validate the server SSL certificate. We do not want that and there is currently no option to
        // remove that offered by the testcontainers API. We could may be use system properties but this would impact
        // the whole VM in which the IT test runs. We prefer for the moment just not use the wait strategy and replace
        // it with a dummy sleep in the caller ESOpenDistroRule to let the docker container initialize. That is why it
        // is commented here after.
//        setWaitStrategy(httpWaitStrategy.withStartupTimeout(Duration.ofMinutes(2)));
//        setWaitStrategy(httpWaitStrategy.withStartupTimeout(Duration.ofSeconds(10)));
    }

    public String getHostPortString() {
        return getContainerIpAddress() + ":" + getMappedPort(ELASTICSEARCH_OPENDISTRO_DEFAULT_PORT);
    }

    public String getHostAddress() {
        return getContainerIpAddress();
    }

    public int getPort() {
        return getMappedPort(ELASTICSEARCH_OPENDISTRO_DEFAULT_PORT);
    }

    public InetSocketAddress getTcpHost() {
        return new InetSocketAddress(getContainerIpAddress(), getMappedPort(ELASTICSEARCH_OPENDISTRO_DEFAULT_TCP_PORT));
    }
}
