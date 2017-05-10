package com.hurence.logisland.service.elasticsearchasaservice;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.processor.elasticsearchasaservice.ElasticsearchClientService;
import com.hurence.logisland.processor.elasticsearchasaservice.put.ElasticsearchPutRecord;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({ "hbase", "client"})
@CapabilityDescription("Implementation of HBaseClientService for HBase 1.1.2. This service can be configured by providing " +
        "a comma-separated list of configuration files, or by specifying values for the other properties. If configuration files " +
        "are provided, they will be loaded first, and the values of the additional properties will override the values from " +
        "the configuration files. In addition, any user defined properties on the processor will also be passed to the HBase " +
        "configuration.")
@DynamicProperty(name="The name of an HBase configuration property.", value="The value of the given HBase configuration property.",
        description="These properties will be set on the HBase configuration after loading any provided configuration files.")
public class Elasticsearch_2_3_3_ClientService extends AbstractControllerService implements ElasticsearchClientService {

    private volatile Client esClient;
    private volatile List<InetSocketAddress> esHosts;
    private volatile String authToken;
    private volatile BulkProcessor bulkProcessor;
    private volatile Map<String/*id*/, String/*errors*/> errors = new HashMap<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(BULK_BACK_OFF_POLICY);
        props.add(BULK_THROTTLING_DELAY);
        props.add(BULK_RETRY_NUMBER);
        props.add(BATCH_SIZE);
        props.add(BULK_SIZE);
        props.add(FLUSH_INTERVAL);
        props.add(CONCURRENT_REQUESTS);
        props.add(CLUSTER_NAME);
        props.add(PING_TIMEOUT);
        props.add(SAMPLER_INTERVAL);
        props.add(USERNAME);
        props.add(PASSWORD);
        props.add(PROP_SHIELD_LOCATION);
        props.add(HOSTS);
        props.add(PROP_SSL_CONTEXT_SERVICE);
        props.add(CHARSET);

        return Collections.unmodifiableList(props);
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException  {
        try {
            createElasticsearchClient(context);
            createBulkProcessor(context);
        }catch (Exception e){
            throw new InitializationException(e);
        }
    }

    /**
     * Instantiate ElasticSearch Client. This chould be called by subclasses' @OnScheduled method to create a client
     * if one does not yet exist. If called when scheduled, closeClient() should be called by the subclasses' @OnStopped
     * method so the client will be destroyed when the processor is stopped.
     *
     * @param context The context for this processor
     * @throws ProcessException if an error occurs while creating an Elasticsearch client
     */
    private void createElasticsearchClient(ControllerServiceInitializationContext context) throws ProcessException {

        if (esClient != null) {
            return;
        }

        try {
            final String clusterName = context.getPropertyValue(CLUSTER_NAME).asString();
            final String pingTimeout = context.getPropertyValue(PING_TIMEOUT).asString();
            final String samplerInterval = context.getPropertyValue(SAMPLER_INTERVAL).asString();
            final String username = context.getPropertyValue(USERNAME).asString();
            final String password = context.getPropertyValue(PASSWORD).asString();

          /*  final SSLContextService sslService =
                    context.getPropertyValue(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
*/
            Settings.Builder settingsBuilder = Settings.settingsBuilder()
                    .put("cluster.name", clusterName)
                    .put("client.transport.ping_timeout", pingTimeout)
                    .put("client.transport.nodes_sampler_interval", samplerInterval);

            String shieldUrl = context.getPropertyValue(PROP_SHIELD_LOCATION).asString();
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

            final String hosts = context.getPropertyValue(HOSTS).asString();
            esHosts = getEsHosts(hosts);

            if (esHosts != null) {
                for (final InetSocketAddress host : esHosts) {
                    try {
                        transportClient.addTransportAddress(new InetSocketTransportAddress(host));
                    } catch (IllegalArgumentException iae) {
                        getLogger().error("Could not add transport address {}", new Object[]{host});
                    }
                }
            }
            esClient = transportClient;

        } catch (Exception e) {
            getLogger().error("Failed to create Elasticsearch client due to {}", new Object[]{e}, e);
            throw new RuntimeException(e);
        }
    }

    private TransportClient getTransportClient(Settings.Builder settingsBuilder, String shieldUrl,
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
                getLogger().debug("Did not detect Elasticsearch Shield plugin, secure connections and/or authorization will not be available");
            }
        } else {
            //logger.debug("No Shield plugin location specified, secure connections and/or authorization will not be available");
        }
        TransportClient transportClient = builder.settings(settingsBuilder.build()).build();
        Thread.currentThread().setContextClassLoader(originalClassLoader);
        return transportClient;
    }


    /**
     * Get the ElasticSearch hosts.
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

    private void createBulkProcessor(ControllerServiceInitializationContext context)
    {
        if (bulkProcessor != null) {
            return;
        }

        /**
         * create the bulk processor
         */
        bulkProcessor = BulkProcessor.builder(
                esClient,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long l, BulkRequest bulkRequest) {
                        getLogger().debug("Going to execute bulk [id:{}] composed of {} actions", new Object[]{l, bulkRequest.numberOfActions()});
                    }

                    @Override
                    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                        getLogger().debug("Executed bulk [id:{}] composed of {} actions", new Object[]{l, bulkRequest.numberOfActions()});
                        if (bulkResponse.hasFailures()) {
                            getLogger().warn("There was failures while executing bulk [id:{}]," +
                                            " done bulk request in {} ms with failure = {}",
                                            new Object[]{l, bulkResponse.getTookInMillis(), bulkResponse.buildFailureMessage()});
                            for (BulkItemResponse item : bulkResponse.getItems()) {
                                if (item.isFailed()) {
                                    errors.put(item.getId(), item.getFailureMessage());
                                }
                            }
                        }
                    }

                    @Override
                    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                        getLogger().error("something went wrong while bulk loading events to es : {}", new Object[]{throwable.getMessage()});
                    }

                })
                .setBulkActions(context.getPropertyValue(BATCH_SIZE).asInteger())
                .setBulkSize(new ByteSizeValue(context.getPropertyValue(BULK_SIZE).asInteger(), ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(context.getPropertyValue(FLUSH_INTERVAL).asInteger()))
                .setConcurrentRequests(context.getPropertyValue(CONCURRENT_REQUESTS).asInteger())
                .setBackoffPolicy(getBackOffPolicy(context))
                .build();
    }

    /**
     * set up BackoffPolicy
     */
    private BackoffPolicy getBackOffPolicy(ControllerServiceInitializationContext context)
    {
        BackoffPolicy backoffPolicy = BackoffPolicy.exponentialBackoff();
        if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(DEFAULT_EXPONENTIAL_BACKOFF_POLICY.getValue())) {
            backoffPolicy = BackoffPolicy.exponentialBackoff();
        } else if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(EXPONENTIAL_BACKOFF_POLICY.getValue())) {
            backoffPolicy = BackoffPolicy.exponentialBackoff(
                    TimeValue.timeValueMillis(context.getPropertyValue(BULK_THROTTLING_DELAY).asLong()),
                    context.getPropertyValue(BULK_RETRY_NUMBER).asInteger()
            );
        } else if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(CONSTANT_BACKOFF_POLICY.getValue())) {
            backoffPolicy = BackoffPolicy.constantBackoff(
                    TimeValue.timeValueMillis(context.getPropertyValue(BULK_THROTTLING_DELAY).asLong()),
                    context.getPropertyValue(BULK_RETRY_NUMBER).asInteger()
            );
        } else if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(NO_BACKOFF_POLICY.getValue())) {
            backoffPolicy = BackoffPolicy.noBackoff();
        }
        return backoffPolicy;
    }

    @Override
    public void put(ElasticsearchPutRecord elasticsearchPutRecord) {
        // dump event to a JSON format
        String document = ElasticsearchRecordConverter.convert(elasticsearchPutRecord.getRecord());
        // add it to the bulk
        IndexRequestBuilder result = esClient
                .prepareIndex(elasticsearchPutRecord.getDocIndex(), elasticsearchPutRecord.getDocType())
                .setId(elasticsearchPutRecord.getRecord().getId())
                .setSource(document).setOpType(IndexRequest.OpType.INDEX);
        bulkProcessor.add(result.request());
    }

    @OnDisabled
    public void shutdown() {
        if (bulkProcessor != null) {
            bulkProcessor.flush();
            try {
                if (!bulkProcessor.awaitClose(10, TimeUnit.SECONDS)) {
                    getLogger().error("some request could not be send to es because of time out");
                } else {
                    getLogger().info("all requests have been submitted to es");
                }
            } catch (InterruptedException e) {
                getLogger().error(e.getMessage());
            }
        }

        if (esClient != null) {
            getLogger().info("Closing ElasticSearch Client");
            esClient.close();
            esClient = null;
        }
    }

}
