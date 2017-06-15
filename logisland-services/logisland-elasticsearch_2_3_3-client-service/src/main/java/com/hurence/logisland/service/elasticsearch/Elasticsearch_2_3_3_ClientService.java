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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecord;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetResponseRecord;
import com.hurence.logisland.record.Record;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

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

@Tags({ "elasticsearch", "client"})
@CapabilityDescription("Implementation of ElasticsearchClientService for Elasticsearch 2.3.3.")
public class Elasticsearch_2_3_3_ClientService extends AbstractControllerService implements ElasticsearchClientService {

    protected volatile Client esClient;
    private volatile List<InetSocketAddress> esHosts;
    private volatile String authToken;
    protected volatile BulkProcessor bulkProcessor;
    protected volatile Map<String/*id*/, String/*errors*/> errors = new HashMap<>();

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
        synchronized(this) {
            try {
                createElasticsearchClient(context);
                createBulkProcessor(context);
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
    protected void createElasticsearchClient(ControllerServiceInitializationContext context) throws ProcessException {
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

    protected void createBulkProcessor(ControllerServiceInitializationContext context)
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
    public void flushBulkProcessor() {
        bulkProcessor.flush();
    }

    @Override
    public void bulkPut(String docIndex, String docType, String document, Optional<String> OptionalId) {
        // add it to the bulk
        IndexRequestBuilder result = esClient
                .prepareIndex(docIndex, docType)
                .setSource(document)
                .setOpType(IndexRequest.OpType.INDEX);
        if(OptionalId.isPresent())
        {
            result.setId(OptionalId.get());
        }
        bulkProcessor.add(result.request());
    }

    @Override
    public void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> OptionalId) {
        // add it to the bulk
        IndexRequestBuilder result = esClient
                .prepareIndex(docIndex, docType)
                .setSource(document)
                .setOpType(IndexRequest.OpType.INDEX);
        if(OptionalId.isPresent())
        {
            result.setId(OptionalId.get());
        }
        bulkProcessor.add(result.request());
    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords){

        List<MultiGetResponseRecord> multiGetResponseRecords = new ArrayList<>();

        MultiGetRequestBuilder multiGetRequestBuilder = esClient.prepareMultiGet();

        for (MultiGetQueryRecord multiGetQueryRecord : multiGetQueryRecords)
        {
            String index = multiGetQueryRecord.getIndexName();
            String type = multiGetQueryRecord.getTypeName();
            List<String> documentIds = multiGetQueryRecord.getDocumentIds();
            String[] fieldsToInclude = multiGetQueryRecord.getFieldsToInclude();
            String[] fieldsToExclude = multiGetQueryRecord.getFieldsToExclude();
            if ((fieldsToInclude != null && fieldsToInclude.length > 0) || (fieldsToExclude != null && fieldsToExclude.length > 0)) {
                for (String documentId : documentIds) {
                    MultiGetRequest.Item item = new MultiGetRequest.Item(index, type, documentId);
                    item.fetchSourceContext(new FetchSourceContext(fieldsToInclude, fieldsToExclude));
                    multiGetRequestBuilder.add(item);
                }
            } else {
                multiGetRequestBuilder.add(index, type, documentIds);
            }
        }

        MultiGetResponse multiGetItemResponses = null;
        try {
            multiGetItemResponses = multiGetRequestBuilder.get();
        } catch (ActionRequestValidationException e) {
            getLogger().error("MultiGet query failed : {}", new Object[]{e.getMessage()});
        }

        if (multiGetItemResponses != null) {
            for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
                GetResponse response = itemResponse.getResponse();
                if (response != null && response.isExists()) {
                    Map<String,Object> responseMap = response.getSourceAsMap();
                    Map<String,String> retrievedFields = new HashMap<>();
                    responseMap.forEach((k,v) -> {if (v!=null) retrievedFields.put(k, v.toString());});
                    multiGetResponseRecords.add(new MultiGetResponseRecord(response.getIndex(), response.getType(), response.getId(), retrievedFields));
                }
            }
        }

        return multiGetResponseRecords;
    }

    @Override
    public boolean existsIndex(String indexName) throws IOException {
        // could also use  client.admin().indices().prepareExists(indexName).execute().get();
        IndicesExistsResponse ersp = IndicesExistsAction.INSTANCE.newRequestBuilder(esClient).setIndices(indexName).get();
        // TODO TK : is the following better ?
        boolean exists = esClient.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
        return ersp.isExists();
    }

    @Override
    public void refreshIndex(String indexName) throws Exception {
        esClient.admin().indices().prepareRefresh(indexName).execute().get();
    }

    @Override
    public void saveAsync(String indexName, String doctype, Map<String, Object> doc) throws Exception {
        esClient.prepareIndex(indexName, doctype).setSource(doc).execute().get();
    }

    @Override
    public void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception {
        esClient.prepareIndex(indexName, doctype).setSource(doc).execute().get();
        refreshIndex(indexName);
    }

    @Override
    public long countIndex(String indexName) throws Exception {
        // There is no "count" method; instead make a "search for all" and set the desired number of returned
        // records to zero. No actual hits get returned, but the "metadata" for the result includes the total
        // number of matched records, ie the size.
        SearchResponse rsp = esClient.prepareSearch(indexName)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(0)
                .execute().get();
        return rsp.getHits().getTotalHits();
    }

    @Override
    public void createIndex(int numShards, int numReplicas, String indexName) throws IOException {
        // Define the index itself
        CreateIndexRequestBuilder builder = esClient.admin().indices().prepareCreate(indexName);
        builder.setSettings(Settings.builder()
                .put("number_of_shards", numShards)
                .put("number_of_replicas", numReplicas)
                .build());

        try {
            CreateIndexResponse rsp = builder.execute().get();
            if (!rsp.isAcknowledged()) {
                throw new IOException("Elasticsearch index definition not acknowledged");
            }
            getLogger().info("Created index {}", new Object[]{indexName});
        } catch (Exception e) {
            getLogger().error("Failed to create ES index", e);
            throw new IOException("Failed to create ES index", e);
        }
    }

    @Override
    public void dropIndex(String indexName) throws IOException {
        try {
            esClient.admin().indices().prepareDelete(indexName).execute().get();
            getLogger().info("Delete index {}", new Object[]{indexName});
        } catch (Exception e) {
            throw new IOException(String.format("Unable to delete index %s", indexName), e);
        }
    }

    @Override
    public void copyIndex(String reindexScrollTimeout, String srcIndex, String dstIndex)
            throws IOException {

        SearchResponse scrollResp = esClient.prepareSearch(srcIndex)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setScroll(reindexScrollTimeout)
                .setQuery(QueryBuilders.matchAllQuery()) // Match all query
                .setSize(100) // 100 hits per shard will be returned for each scroll
                .execute().actionGet();

        AtomicBoolean failed = new AtomicBoolean(false);

        // A user of a BulkProcessor just keeps adding requests to it, and the BulkProcessor itself decides when
        // to send a request to the ES nodes, based on its configuration settings. Calls can be triggerd by number
        // of queued requests, total size of queued requests, and time since previous request. The defaults for
        // these settings are all sensible, so are not overridden here. The BulkProcessor has an internal threadpool
        // which allows it to send multiple batches concurrently; the default is "1" meaning that a single completed
        // batch can be sending in the background while a new batch is being built. When the non-active batch is
        // "full", the add call blocks until the background batch completes.

        while (true) {
            if (scrollResp.getHits().getHits().length == 0) {
                // No more results
                break;
            }

            for (SearchHit hit : scrollResp.getHits()) {
                IndexRequest request = new IndexRequest(dstIndex, hit.type(), hit.id());
                Map<String, Object> source = hit.getSource();
                request.source(source);
                bulkProcessor.add(request);
            }

            String scrollId = scrollResp.getScrollId();
            scrollResp = esClient.prepareSearchScroll(scrollId)
                    .setScroll(reindexScrollTimeout)
                    .execute().actionGet();
        }

        getLogger().info("Reindex completed");
    }

    @Override
    public void createAlias(String indexName, String aliasName) throws IOException {
        IndicesAliasesRequestBuilder builder = esClient.admin().indices().prepareAliases().addAlias(indexName, aliasName);
        try {
            IndicesAliasesResponse rsp = builder.execute().get();
            if (!rsp.isAcknowledged()) {
                throw new IOException(String.format(
                        "Creation of elasticsearch alias '%s' for index '%s' not acknowledged.", aliasName, indexName));
            }
        } catch (IOException e) {
            getLogger().error("Failed to create elasticsearch alias {} for index {}", new Object[] {aliasName, indexName, e});
            throw e;
        } catch (ExecutionException | InterruptedException e) {
            String msg = String.format("Failed to create elasticsearch alias '%s' for index '%s'", aliasName, indexName);
            throw new IOException(msg, e);
        }
    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString)
            throws IOException {

        PutMappingRequestBuilder builder = esClient.admin().indices().preparePutMapping(indexName);
        builder.setType(doctype).setSource(mappingAsJsonString);

        try {
            PutMappingResponse rsp = builder.execute().get();
            if (!rsp.isAcknowledged()) {
                throw new IOException("Elasticsearch mapping definition not acknowledged");
            }
            return true;
        } catch (Exception e) {
            getLogger().error("Failed to load ES mapping {} for index {}", new Object[]{doctype, indexName, e});
            // This is an error that can be fixed by providing alternative inputs so return boolean rather
            // than throwing an exception.
            return false;
        }
    }

    @Override
    public String convertRecordToString(Record record) {
        return ElasticsearchRecordConverter.convertToString(record);
    }

    @Override
    public long searchNumberOfHits(String docIndex, String docType, String docName, String docValue)
    {
        SearchResponse searchResponse = esClient.prepareSearch(docIndex)
                .setTypes(docType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery(docName, docValue))
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();

        long numberOfHits = searchResponse.getHits().getTotalHits();

        return numberOfHits;
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
