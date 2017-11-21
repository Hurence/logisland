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
package com.hurence.logisland.service.solr;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecord;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetResponseRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Tags({ "solr", "client"})
@CapabilityDescription("Implementation of ElasticsearchClientService for Solr 5.5.5.")
public class Solr_5_5_5_SearchClientService extends AbstractControllerService implements ElasticsearchClientService {

    protected volatile SolrClient solrClient;
    private volatile List<InetSocketAddress> esHosts;
    private volatile String authToken;
    protected volatile Map<String/*id*/, String/*errors*/> errors = new HashMap<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
//        props.add(BULK_BACK_OFF_POLICY);
//        props.add(BULK_THROTTLING_DELAY);
//        props.add(BULK_RETRY_NUMBER);
//        props.add(BATCH_SIZE);
//        props.add(BULK_SIZE);
//        props.add(FLUSH_INTERVAL);
//        props.add(CONCURRENT_REQUESTS);
//        props.add(CLUSTER_NAME);
//        props.add(PING_TIMEOUT);
//        props.add(SAMPLER_INTERVAL);
//        props.add(USERNAME);
//        props.add(PASSWORD);
//        props.add(PROP_SHIELD_LOCATION);
//        props.add(HOSTS);
//        props.add(PROP_SSL_CONTEXT_SERVICE);
//        props.add(CHARSET);

        return Collections.unmodifiableList(props);
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException  {
        synchronized(this) {
            try {
                createSolrClient(context);
                //createBulkProcessor(context);
            }catch (Exception e){
                throw new InitializationException(e);
            }
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
    protected void createSolrClient(ControllerServiceInitializationContext context) throws ProcessException {
        if (solrClient != null) {
            return;
        }

//        try {
//            final String clusterName = context.getPropertyValue(CLUSTER_NAME).asString();
//            final String pingTimeout = context.getPropertyValue(PING_TIMEOUT).asString();
//            final String samplerInterval = context.getPropertyValue(SAMPLER_INTERVAL).asString();
//            final String username = context.getPropertyValue(USERNAME).asString();
//            final String password = context.getPropertyValue(PASSWORD).asString();
//
//          /*  final SSLContextService sslService =
//                    context.getPropertyValue(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
//*/
//            Settings.Builder settingsBuilder = Settings.builder()
//                    .put("cluster.name", clusterName)
//                    .put("client.transport.ping_timeout", pingTimeout)
//                    .put("client.transport.nodes_sampler_interval", samplerInterval);
//
//            String shieldUrl = context.getPropertyValue(PROP_SHIELD_LOCATION).asString();
//          /*  if (sslService != null) {
//                settingsBuilder.setField("shield.transport.ssl", "true")
//                        .setField("shield.ssl.keystore.path", sslService.getKeyStoreFile())
//                        .setField("shield.ssl.keystore.password", sslService.getKeyStorePassword())
//                        .setField("shield.ssl.truststore.path", sslService.getTrustStoreFile())
//                        .setField("shield.ssl.truststore.password", sslService.getTrustStorePassword());
//            }*/
//
//            // Set username and password for Shield
//            if (!StringUtils.isEmpty(username)) {
//                StringBuffer shieldUser = new StringBuffer(username);
//                if (!StringUtils.isEmpty(password)) {
//                    shieldUser.append(":");
//                    shieldUser.append(password);
//                }
//                settingsBuilder.put("shield.user", shieldUser);
//
//            }
//
//            TransportClient transportClient = getTransportClient(settingsBuilder, shieldUrl, username, password);
//
//            final String hosts = context.getPropertyValue(HOSTS).asString();
//            esHosts = getEsHosts(hosts);
//
//            if (esHosts != null) {
//                for (final InetSocketAddress host : esHosts) {
//                    try {
//                        transportClient.addTransportAddress(new InetSocketTransportAddress(host));
//                    } catch (IllegalArgumentException iae) {
//                        getLogger().error("Could not add transport address {}", new Object[]{host});
//                    }
//                }
//            }
//            esClient = transportClient;
//
//        } catch (Exception e) {
//            getLogger().error("Failed to create Elasticsearch client due to {}", new Object[]{e}, e);
//            throw new RuntimeException(e);
//        }
    }

//    private TransportClient getTransportClient(Settings.Builder settingsBuilder, String shieldUrl,
//                                               String username, String password)
//            throws MalformedURLException {
//
//        // See if the Elasticsearch Shield JAR location was specified, and add the plugin if so. Also create the
//        // authorization token if username and password are supplied.
//        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
//        if (!StringUtils.isBlank(shieldUrl)) {
//            ClassLoader shieldClassLoader =
//                    new URLClassLoader(new URL[]{new File(shieldUrl).toURI().toURL()}, this.getClass().getClassLoader());
//            Thread.currentThread().setContextClassLoader(shieldClassLoader);
//
//            try {
//                //Class shieldPluginClass = Class.forName("org.elasticsearch.shield.ShieldPlugin", true, shieldClassLoader);
//                //builder = builder.addPlugin(shieldPluginClass);
//
//                if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
//
//                    // Need a couple of classes from the Shield plugin to build the token
//                    Class usernamePasswordTokenClass =
//                            Class.forName("org.elasticsearch.shield.authc.support.UsernamePasswordToken", true, shieldClassLoader);
//
//                    Class securedStringClass =
//                            Class.forName("org.elasticsearch.shield.authc.support.SecuredString", true, shieldClassLoader);
//
//                    Constructor<?> securedStringCtor = securedStringClass.getConstructor(char[].class);
//                    Object securePasswordString = securedStringCtor.newInstance(password.toCharArray());
//
//                    Method basicAuthHeaderValue = usernamePasswordTokenClass.getMethod("basicAuthHeaderValue", String.class, securedStringClass);
//                    authToken = (String) basicAuthHeaderValue.invoke(null, username, securePasswordString);
//                }
//            } catch (ClassNotFoundException
//                    | NoSuchMethodException
//                    | InstantiationException
//                    | IllegalAccessException
//                    | InvocationTargetException shieldLoadException) {
//                getLogger().debug("Did not detect Elasticsearch Shield plugin, secure connections and/or authorization will not be available");
//            }
//        } else {
//            //logger.debug("No Shield plugin location specified, secure connections and/or authorization will not be available");
//        }
//        TransportClient transportClient = new PreBuiltTransportClient(settingsBuilder.build());
//        Thread.currentThread().setContextClassLoader(originalClassLoader);
//        return transportClient;
//    }


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


    @Override
    public void flushBulkProcessor() {
    }

    @Override
    public void bulkPut(String docIndex, String docType, String document, Optional<String> OptionalId) {

    }

    @Override
    public void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> OptionalId) {

    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords){
        return null;
    }

    @Override
    public boolean existsIndex(String indexName) throws IOException {
        return true;
    }

    @Override
    public void refreshIndex(String indexName) throws Exception {

    }

    @Override
    public void saveAsync(String indexName, String doctype, Map<String, Object> doc) throws Exception {

    }

    @Override
    public void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception {

    }

    @Override
    public long countIndex(String indexName) throws Exception {
        return 0;

    }

    @Override
    public void createIndex(int numShards, int numReplicas, String indexName) throws IOException {

    }

    @Override
    public void dropIndex(String indexName) throws IOException {

    }

    @Override
    public void copyIndex(String reindexScrollTimeout, String srcIndex, String dstIndex)
            throws IOException {
    }

    @Override
    public void createAlias(String indexName, String aliasName) throws IOException {

    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString)
            throws IOException {
        return false;
    }

    @Override
    public String convertRecordToString(Record record) {
        return "";

    }

    @Override
    public long searchNumberOfHits(String docIndex, String docType, String docName, String docValue)
    {
        return 0;
    }

    @OnDisabled
    public void shutdown() {

    }

}
