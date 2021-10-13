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
package com.hurence.junit5.extension;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashSet;

/**
 * A JUnit rule which starts an embedded elastic-search docker container.
 * <p>
 * Tests which use this rule will run relatively slowly, and should only be used when more conventional unit tests are
 * not sufficient - eg when testing DAO-specific code.
 * </p>
 */
public class Es7DockerExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private static Logger logger = LoggerFactory.getLogger(Es7DockerExtension.class);
    public final static String ES_SERVICE_NAME = "elasticsearch";
    public final static int ES_PORT_HTTP = 9200;
    public final static int ES_PORT_TCP = 9300;
    private static final HashSet<Class> INJECTABLE_TYPES = new HashSet<Class>() {
        {
            add(RestHighLevelClient.class);
            add(DockerComposeContainer.class);
        }
    };
    /**
     * The internal-transport client that talks to the local node.
     */
    private RestHighLevelClient client;
    private DockerComposeContainer dockerComposeContainer;

    /**
     * Return the object through which operations can be performed on the ES cluster.
     */
    public RestHighLevelClient getClient() {
        return client;
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        if (getClient() != null) getClient().close();
        if (dockerComposeContainer != null) dockerComposeContainer.stop();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        this.dockerComposeContainer = new DockerComposeContainer(
                new File(getClass().getResource("/docker-compose-es-test.yml").getFile())
        )
                .withExposedService(ES_SERVICE_NAME, ES_PORT_HTTP, Wait.forListeningPort())
                .withExposedService(ES_SERVICE_NAME, ES_PORT_TCP, Wait.forListeningPort());
        logger.info("Starting docker compose");
        this.dockerComposeContainer.start();

        String httpUrl = getEsHttpUrl(dockerComposeContainer);
        String tcpUrl = getEsTcpUrl(dockerComposeContainer);
        logger.info("httpUrl of es http://" + httpUrl);
        logger.info("tcpUrl of es tcp://" + tcpUrl);

        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(dockerComposeContainer.getServiceHost(ES_SERVICE_NAME, ES_PORT_HTTP),
                                dockerComposeContainer.getServicePort(ES_SERVICE_NAME, ES_PORT_HTTP),
                                "http")
                ));
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return INJECTABLE_TYPES.contains(parameterType(parameterContext));
    }

    private Class<?> parameterType(ParameterContext parameterContext) {
        return parameterContext.getParameter().getType();
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Class<?> type = parameterType(parameterContext);
        if (type == RestHighLevelClient.class) {
            return getClient();
        }
        if (type == DockerComposeContainer.class) {
            return dockerComposeContainer;
        }
        throw new IllegalStateException("Looks like the ParameterResolver needs a fix...");
    }

    private static Logger getLogger() {
        return logger;
    }

    public static String getEsHttpUrl(DockerComposeContainer dockerComposeContainer) {
        return dockerComposeContainer.getServiceHost(ES_SERVICE_NAME, ES_PORT_HTTP)
                + ":" +
                dockerComposeContainer.getServicePort(ES_SERVICE_NAME, ES_PORT_HTTP);
    }

    public static String getEsTcpUrl(DockerComposeContainer dockerComposeContainer) {
        return dockerComposeContainer.getServiceHost(ES_SERVICE_NAME, ES_PORT_TCP)
                + ":" +
                dockerComposeContainer.getServicePort(ES_SERVICE_NAME, ES_PORT_TCP);
    }
}