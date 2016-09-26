/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.hurence.logisland.processor.elasticsearch;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.ValidationResult;
import com.hurence.logisland.component.Validator;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.KafkaStreamProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.validators.StandardValidators;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


public abstract class AbstractElasticsearchProcessor extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(AbstractElasticsearchProcessor.class);
    /**
     * This validator ensures the Elasticsearch hosts property is a valid list of hostname:port entries
     */
    private static final Validator HOSTNAME_PORT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input) {
            final List<String> esList = Arrays.asList(input.split(","));
            for (String hostnamePort : esList) {
                String[] addresses = hostnamePort.split(":");
                // Protect against invalid input like http://127.0.0.1:9300 (URL scheme should not be there)
                if (addresses.length != 2) {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Must be in hostname:port form (no scheme such as http://").valid(false).build();
                }
            }
            return new ValidationResult.Builder().subject(subject).input(input).explanation(
                    "Valid cluster definition").valid(true).build();
        }
    };

    protected static final PropertyDescriptor CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("cluster.name")
            .description("Name of the ES cluster (for example, elasticsearch_brew). Defaults to 'elasticsearch'")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("elasticsearch")
            .build();

    protected static final PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
            .name("hosts")
            .description("ElasticSearch Hosts, which should be comma separated and colon for hostname/port "
                    + "host1:port,host2:port,....  For example testcluster:9300.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(HOSTNAME_PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl.context.service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections. This service only applies if the Shield plugin is available.")
            .required(false)
            .build();

    public static final PropertyDescriptor PROP_SHIELD_LOCATION = new PropertyDescriptor.Builder()
            .name("shield.location")
            .description("Specifies the path to the JAR for the Elasticsearch Shield plugin. "
                    + "If the Elasticsearch cluster has been secured with the Shield plugin, then the Shield plugin "
                    + "JAR must also be available to this processor. Note: Do NOT place the Shield JAR into NiFi's "
                    + "lib/ directory, doing so will prevent the Shield plugin from being loaded.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .description("Username to access the Elasticsearch cluster")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .description("Password to access the Elasticsearch cluster")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor PING_TIMEOUT = new PropertyDescriptor.Builder()
            .name("ping.timeout")
            .description("The ping timeout used to determine when a node is unreachable. " +
                    "For example, 5s (5 seconds). If non-local recommended is 30s")
            .required(true)
            .defaultValue("5s")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor SAMPLER_INTERVAL = new PropertyDescriptor.Builder()
            .name("sampler.interval")
            .description("How often to sample / ping the nodes listed and connected. For example, 5s (5 seconds). "
                    + "If non-local recommended is 30s.")
            .required(true)
            .defaultValue("5s")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("charset")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    protected AtomicReference<Client> esClient = new AtomicReference<>();
    protected List<InetSocketAddress> esHosts;
    protected String authToken;

/*
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Ensure that if username or password is set, then the other is too
        Map<PropertyDescriptor, String> propertyMap = validationContext.getProperties();
        if (StringUtils.isEmpty(propertyMap.getField(USERNAME)) != StringUtils.isEmpty(propertyMap.getField(PASSWORD))) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "If username or password is specified, then the other must be specified as well").build());
        }

        return results;
    }*/

    public void setup(ProcessContext context) {
        // Create the client if one does not already exist
        createElasticsearchClient(context);
    }

    /**
     * Instantiate ElasticSearch Client. This chould be called by subclasses' @OnScheduled method to create a client
     * if one does not yet exist. If called when scheduled, closeClient() should be called by the subclasses' @OnStopped
     * method so the client will be destroyed when the processor is stopped.
     *
     * @param context The context for this processor
     * @throws ProcessException if an error occurs while creating an Elasticsearch client
     */
    protected void createElasticsearchClient(ProcessContext context) throws ProcessException {

        if (esClient.get() != null) {
            return;
        }

        try {
            final String clusterName = context.getProperty(CLUSTER_NAME).asString();
            final String pingTimeout = context.getProperty(PING_TIMEOUT).asString();
            final String samplerInterval = context.getProperty(SAMPLER_INTERVAL).asString();
            final String username = context.getProperty(USERNAME).asString();
            final String password = context.getProperty(PASSWORD).asString();

          /*  final SSLContextService sslService =
                    context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
*/
            Settings.Builder settingsBuilder = Settings.settingsBuilder()
                    .put("cluster.name", clusterName)
                    .put("client.transport.ping_timeout", pingTimeout)
                    .put("client.transport.nodes_sampler_interval", samplerInterval);

            String shieldUrl = context.getProperty(PROP_SHIELD_LOCATION).asString();
          /*  if (sslService != null) {
                settingsBuilder.setField("shield.transport.ssl", "true")
                        .setField("shield.ssl.keystore.path", sslService.getKeyStoreFile())
                        .setField("shield.ssl.keystore.password", sslService.getKeyStorePassword())
                        .setField("shield.ssl.truststore.path", sslService.getTrustStoreFile())
                        .setField("shield.ssl.truststore.password", sslService.getTrustStorePassword());
            }*/

            // Set username and password for Shield
            if (!StringUtils.isEmpty(username)) {
                StringBuffer shieldUser = new StringBuffer(username);
                if (!StringUtils.isEmpty(password)) {
                    shieldUser.append(":");
                    shieldUser.append(password);
                }
                settingsBuilder.put("shield.user", shieldUser);

            }

            TransportClient transportClient = getTransportClient(settingsBuilder, shieldUrl, username, password);

            final String hosts = context.getProperty(HOSTS).asString();
            esHosts = getEsHosts(hosts);

            if (esHosts != null) {
                for (final InetSocketAddress host : esHosts) {
                    try {
                        transportClient.addTransportAddress(new InetSocketTransportAddress(host));
                    } catch (IllegalArgumentException iae) {
                        logger.error("Could not add transport address {}", new Object[]{host});
                    }
                }
            }
            esClient.set(transportClient);

        } catch (Exception e) {
            logger.error("Failed to create Elasticsearch client due to {}", new Object[]{e}, e);
            throw new ProcessException(e);
        }
    }

    protected TransportClient getTransportClient(Settings.Builder settingsBuilder, String shieldUrl,
                                                 String username, String password)
            throws MalformedURLException {

        // Create new transport client using the Builder pattern
        TransportClient.Builder builder = TransportClient.builder();

        // See if the Elasticsearch Shield JAR location was specified, and add the plugin if so. Also create the
        // authorization token if username and password are supplied.
        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        if (!StringUtils.isBlank(shieldUrl)) {
            ClassLoader shieldClassLoader =
                    new URLClassLoader(new URL[]{new File(shieldUrl).toURI().toURL()}, this.getClass().getClassLoader());
            Thread.currentThread().setContextClassLoader(shieldClassLoader);

            try {
                Class shieldPluginClass = Class.forName("org.elasticsearch.shield.ShieldPlugin", true, shieldClassLoader);
                builder = builder.addPlugin(shieldPluginClass);

                if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {

                    // Need a couple of classes from the Shield plugin to build the token
                    Class usernamePasswordTokenClass =
                            Class.forName("org.elasticsearch.shield.authc.support.UsernamePasswordToken", true, shieldClassLoader);

                    Class securedStringClass =
                            Class.forName("org.elasticsearch.shield.authc.support.SecuredString", true, shieldClassLoader);

                    Constructor<?> securedStringCtor = securedStringClass.getConstructor(char[].class);
                    Object securePasswordString = securedStringCtor.newInstance(password.toCharArray());

                    Method basicAuthHeaderValue = usernamePasswordTokenClass.getMethod("basicAuthHeaderValue", String.class, securedStringClass);
                    authToken = (String) basicAuthHeaderValue.invoke(null, username, securePasswordString);
                }
            } catch (ClassNotFoundException
                    | NoSuchMethodException
                    | InstantiationException
                    | IllegalAccessException
                    | InvocationTargetException shieldLoadException) {
                logger.debug("Did not detect Elasticsearch Shield plugin, secure connections and/or authorization will not be available");
            }
        } else {
            //logger.debug("No Shield plugin location specified, secure connections and/or authorization will not be available");
        }
        TransportClient transportClient = builder.settings(settingsBuilder.build()).build();
        Thread.currentThread().setContextClassLoader(originalClassLoader);
        return transportClient;
    }

    /**
     * Dispose of ElasticSearch client
     */
    public void closeClient() {
        if (esClient.get() != null) {
            logger.info("Closing ElasticSearch Client");
            esClient.get().close();
            esClient.set(null);
        }
    }

    /**
     * Get the ElasticSearch hosts from a Nifi attribute, e.g.
     *
     * @param hosts A comma-separated list of ElasticSearch hosts (host:port,host2:port2, etc.)
     * @return List of InetSocketAddresses for the ES hosts
     */
    private List<InetSocketAddress> getEsHosts(String hosts) {

        if (hosts == null) {
            return null;
        }
        final List<String> esList = Arrays.asList(hosts.split(","));
        List<InetSocketAddress> esHosts = new ArrayList<>();

        for (String item : esList) {

            String[] addresses = item.split(":");
            final String hostName = addresses[0].trim();
            final int port = Integer.parseInt(addresses[1].trim());

            esHosts.add(new InetSocketAddress(hostName, port));
        }
        return esHosts;
    }


}
