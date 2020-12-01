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
import com.hurence.logisland.annotation.documentation.Category;
import com.hurence.logisland.annotation.documentation.ComponentCategory;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.*;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

@Category(ComponentCategory.DATASTORE)
@Tags({ "elasticsearch", "client"})
@CapabilityDescription("Implementation of ElasticsearchClientService for Elasticsearch 6.6.2.")
public class Elasticsearch_6_6_2_ClientService extends AbstractControllerService implements ElasticsearchClientService {


    protected volatile RestHighLevelClient esClient;
    private volatile HttpHost[] esHosts;
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
        super.init(context);
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
     * Instantiate ElasticSearch Client. This should be called by subclasses' @OnScheduled method to create a client
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
            final String username = context.getPropertyValue(USERNAME).asString();
            final String password = context.getPropertyValue(PASSWORD).asString();
            final String hosts = context.getPropertyValue(HOSTS).asString();

            esHosts = getEsHosts(hosts);

            if (esHosts != null) {

                RestClientBuilder builder = RestClient.builder(esHosts);

                if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

                    builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                @Override
                                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                }
                            });
                }

                esClient = new RestHighLevelClient(builder);
            }

        } catch (Exception e) {
            getLogger().error("Failed to create Elasticsearch client due to {}", new Object[]{e}, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the ElasticSearch hosts.
     *
     * @param hosts A comma-separated list of ElasticSearch hosts (host:port,host2:port2, etc.)
     * @return List of HttpHost for the ES hosts
     */
    private HttpHost[]  getEsHosts(String hosts) {

        if (hosts == null) {
            return null;
        }
        final List<String> esList = Arrays.asList(hosts.split(","));
        HttpHost[] esHosts = new HttpHost[esList.size()];
        int indHost = 0;

        for (String item : esList) {
            String[] addresses = item.split(":");
            final String hostName = addresses[0].trim();
            final int port = Integer.parseInt(addresses[1].trim());

            esHosts[indHost] = new HttpHost(hostName, port);
            indHost++;
        }
        return esHosts;
    }


    protected void createBulkProcessor(ControllerServiceInitializationContext context)
    {
        if (bulkProcessor != null) {
            return;
        }

        // create the bulk processor

       BulkProcessor.Listener listener = new BulkProcessor.Listener() {
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
                            new Object[]{l, bulkResponse.getTook().getMillis(), bulkResponse.buildFailureMessage()});
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

        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (request, bulkListener) -> esClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        bulkProcessor = BulkProcessor.builder(bulkConsumer, listener)
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
    public void bulkFlush() throws DatastoreClientServiceException {
        bulkProcessor.flush();
    }

    @Override
    public void bulkPut(String docIndex, String docType, String document, Optional<String> OptionalId) {
        // add it to the bulk,
        IndexRequest request = new IndexRequest(docIndex, docType)
                .source(document, XContentType.JSON)
                .opType(IndexRequest.OpType.INDEX);

        if(OptionalId.isPresent()){
            request.id(OptionalId.get());
        }

        bulkProcessor.add(request);
    }

    @Override
    public void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> OptionalId) {
        // add it to the bulk
        IndexRequest request = new IndexRequest(docIndex, docType)
                .source(document)
                .opType(IndexRequest.OpType.INDEX);

        if(OptionalId.isPresent()){
            request.id(OptionalId.get());
        }

        bulkProcessor.add(request);
    }

    @Override
    public void bulkDelete(String docIndex, String docType, String id) {
        DeleteRequest request = new DeleteRequest(docIndex, docType, id);
        bulkProcessor.add(request);
    }

    @Override
    public void deleteByQuery(QueryRecord queryRecord) throws DatastoreClientServiceException {
        String[] indices = new String[queryRecord.getCollections().size()];
        DeleteByQueryRequest request = new DeleteByQueryRequest(queryRecord.getCollections().toArray(indices));
        QueryBuilder builder = toQueryBuilder(queryRecord);
        request.setQuery(builder);
        request.setRefresh(queryRecord.getRefresh());
        try {
            BulkByScrollResponse bulkResponse =
                    esClient.deleteByQuery(request, RequestOptions.DEFAULT);
            getLogger().info("deleted {} documents, got {} failure(s).", new Object[]{bulkResponse.getDeleted(), bulkResponse.getBulkFailures().size()});
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("response was {}", new Object[]{bulkResponse});
            }
        } catch (IOException e) {
            getLogger().error("error while deleteByQuery", e);
            throw new DatastoreClientServiceException(e);
        }
    }

    private QueryBuilder toQueryBuilder(QueryRecord queryRecord) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (TermQueryRecord termQuery : queryRecord.getTermQueries()) {
            boolQuery = boolQuery
                    .must(QueryBuilders.termQuery(termQuery.getFieldName(), termQuery.getFieldValue()));
        }
        for (RangeQueryRecord rangeQuery : queryRecord.getRangeQueries()) {
            boolQuery = boolQuery
                    .must(
                            QueryBuilders
                                    .rangeQuery(rangeQuery.getFieldName())
                                    .from(rangeQuery.getFrom(), rangeQuery.isIncludeLower())
                                    .to(rangeQuery.getTo(), rangeQuery.isIncludeUpper())
                    );
        }
        return boolQuery;
    }


    /**
     * Wait until specified collection is ready to be used.
     */
    @Override
    public void waitUntilCollectionReady(String collection, long timeoutMilli) throws DatastoreClientServiceException {
        getIndexHealth(new String[]{collection}, timeoutMilli);
    }

    @Override
    public void waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(String[] indices, long timeoutMilli) throws DatastoreClientServiceException {
        ClusterHealthResponse rsp = getIndexHealth(indices, timeoutMilli);
        if (rsp == null) {
            getLogger().error("index {} seems to not be ready (query failed) !", new Object[]{indices});
            return;
        }
        if (rsp.isTimedOut()) {
            getLogger().error("index {} is not ready !", new Object[]{indices});
        } else {
            if (rsp.getNumberOfPendingTasks() != 0) {
                this.refreshCollections(indices);
            }
        }
    }

    @Override
    public void refreshCollections(String[] indices) throws DatastoreClientServiceException {
        try {
            RefreshRequest request = new RefreshRequest(indices);
            esClient.indices().refresh(request, RequestOptions.DEFAULT);
        } catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }
    }

    private ClusterHealthResponse getIndexHealth(String[] indices, long timeoutMilli) {
        ClusterHealthRequest request = new ClusterHealthRequest(indices)
                .timeout(TimeValue.timeValueMillis(timeoutMilli))
                .waitForGreenStatus()
                .waitForEvents(Priority.LOW);
        ClusterHealthResponse response = null;
        try {
            response = esClient.cluster().health(request, RequestOptions.DEFAULT);
            getLogger().trace("health response for indices {} is {}", new Object[]{indices, response});
        } catch (Exception e) {
            getLogger().error("health query failed : {}", new Object[]{e.getMessage()});
        }
        return response;
    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {

        List<MultiGetResponseRecord> multiGetResponseRecords = new ArrayList<>();

        MultiGetRequest multiGetRequest = new MultiGetRequest();

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
                    item.fetchSourceContext(new FetchSourceContext(true, fieldsToInclude, fieldsToExclude));
                    multiGetRequest.add(item);
                }
            } else {
                for (String documentId : documentIds) {
                    multiGetRequest.add(index, type, documentId);
                }
            }
        }

        MultiGetResponse multiGetItemResponses = null;
        try {
            multiGetItemResponses = esClient.mget(multiGetRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
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
    public boolean existsCollection(String indexName) throws DatastoreClientServiceException {
        boolean exists;
        try {
            GetIndexRequest request = new GetIndexRequest();
            request.indices(indexName);
            exists = esClient.indices().exists(request, RequestOptions.DEFAULT);
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }
        return exists;
    }

    @Override
    public void refreshCollection(String indexName) throws DatastoreClientServiceException {
        try {
            RefreshRequest request = new RefreshRequest(indexName);
            esClient.indices().refresh(request, RequestOptions.DEFAULT);
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception {
        IndexRequest indexRequest = new IndexRequest(indexName, doctype).source(doc);
        esClient.index(indexRequest, RequestOptions.DEFAULT);
        refreshCollection(indexName);
    }


    @Override
    public long countCollection(String indexName) throws DatastoreClientServiceException {
        CountResponse countResponse;
        try {
            CountRequest countRequest = new CountRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            countRequest.source(searchSourceBuilder);
            countResponse = esClient.count(countRequest, RequestOptions.DEFAULT);
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }
        return countResponse.getCount();
    }

    @Override
    public void createCollection(String indexName, int numShards, int numReplicas) throws DatastoreClientServiceException {
        // Define the index itself
        CreateIndexRequest request = new CreateIndexRequest(indexName);

        request.settings(Settings.builder()
                .put("index.number_of_shards", numShards)
                .put("index.number_of_replicas", numReplicas)
        );

        try {
            CreateIndexResponse rsp = esClient.indices().create(request, RequestOptions.DEFAULT);
            if (!rsp.isAcknowledged()) {
                throw new IOException("Elasticsearch index definition not acknowledged");
            }
            getLogger().info("Created index {}", new Object[]{indexName});
        } catch (Exception e) {
            getLogger().error("Failed to create ES index", e);
            throw new DatastoreClientServiceException("Failed to create ES index", e);
        }
    }

    @Override
    public void dropCollection(String indexName) throws DatastoreClientServiceException {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            getLogger().info("Delete index {}", new Object[]{indexName});
            esClient.indices().delete(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new DatastoreClientServiceException(String.format("Unable to delete index %s", indexName), e);
        }
    }

    @Override
    public void copyCollection(String reindexScrollTimeout, String srcIndex, String dstIndex) throws DatastoreClientServiceException {
        try {

            SearchRequest searchRequest = new SearchRequest(srcIndex);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery())
                .size(100);
            searchRequest.source(searchSourceBuilder).scroll(reindexScrollTimeout).searchType(SearchType.QUERY_THEN_FETCH);
            SearchResponse scrollResp = esClient.search(searchRequest, RequestOptions.DEFAULT);

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
                    IndexRequest request = new IndexRequest(dstIndex, hit.getType(), hit.getId());
                    Map<String, Object> source = hit.getSourceAsMap();
                    request.source(source);
                    bulkProcessor.add(request);
                }

                String scrollId = scrollResp.getScrollId();
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(reindexScrollTimeout);
                scrollResp = esClient.scroll(scrollRequest, RequestOptions.DEFAULT);
            }

            getLogger().info("Reindex completed");
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public void createAlias(String indexName, String aliasName) throws DatastoreClientServiceException {
        try {

            IndicesAliasesRequest request = new IndicesAliasesRequest();
            IndicesAliasesRequest.AliasActions aliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(indexName)
                            .alias(aliasName);
            request.addAliasAction(aliasAction);
            AcknowledgedResponse rsp = esClient.indices().updateAliases(request, RequestOptions.DEFAULT);

            if (!rsp.isAcknowledged()) {
                throw new DatastoreClientServiceException(String.format(
                        "Creation of elasticsearch alias '%s' for index '%s' not acknowledged.", aliasName, indexName));
            }
        } catch (DatastoreClientServiceException e) {
            getLogger().error("Failed to create elasticsearch alias {} for index {}", new Object[]{aliasName, indexName, e});
            throw e;
        }
        catch (Exception e){
            String msg = String.format("Failed to create elasticsearch alias '%s' for index '%s'", aliasName, indexName);
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws DatastoreClientServiceException {
        PutMappingRequest request = new PutMappingRequest(indexName);
        request.type(doctype);
        request.source(mappingAsJsonString, XContentType.JSON);

        try {
            AcknowledgedResponse rsp = esClient.indices().putMapping(request, RequestOptions.DEFAULT);
            if (!rsp.isAcknowledged()) {
                throw new DatastoreClientServiceException("Elasticsearch mapping definition not acknowledged");
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
    public void bulkPut(String indexTypeName, Record record) throws DatastoreClientServiceException {

        final List<String> indexType = Arrays.asList(indexTypeName.split(","));

        if (indexType.size() == 2) {
            String indexName = indexType.get(0);
            String typeName = indexType.get(1);
            this.bulkPut(indexName, typeName, ElasticsearchRecordConverter.convertToString(record), Optional.of(record.getId()));
        }
        else {
            throw new DatastoreClientServiceException("The Elastic Search type name is missing. Please add at least default.type to the processor parameters");
        }

    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for ElasticSearch 6.6.2");
    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for ElasticSearch 6.6.2");
    }

    @Override
    public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for ElasticSearch 6.6.2");
    }

    @Override
    public Collection<Record> query(String query) {
        throw new NotImplementedException("Not yet supported for ElasticSearch 6.6.2");
    }

    @Override
    public long queryCount(String query) {
        throw new NotImplementedException("Not yet supported for ElasticSearch 6.6.2");
    }

    @Override
    public String convertRecordToString(Record record) {
        return ElasticsearchRecordConverter.convertToString(record);
    }

    @Override
    public long searchNumberOfHits(String docIndex, String docType, String docName, String docValue) {

        long numberOfHits;

        try {
            SearchRequest searchRequest = new SearchRequest(docIndex);

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            sourceBuilder.query(QueryBuilders.termQuery(docName, docValue));
            sourceBuilder.from(0);
            sourceBuilder.size(60);
            sourceBuilder.explain(true);

            searchRequest.source(sourceBuilder);

            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            numberOfHits = searchResponse.getHits().getTotalHits();
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }

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
            try {
                esClient.close();
            }
            catch (Exception e){
                throw new DatastoreClientServiceException(e);
            }
            esClient = null;
        }
    }


}
