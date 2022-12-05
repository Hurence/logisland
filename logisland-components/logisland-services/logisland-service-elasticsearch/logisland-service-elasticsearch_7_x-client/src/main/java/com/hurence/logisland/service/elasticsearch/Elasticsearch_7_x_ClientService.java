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
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.*;
import com.hurence.logisland.service.datastore.model.*;
import com.hurence.logisland.service.datastore.model.bool.*;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
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
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.beans.Transient;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Tags({ "elasticsearch", "client"})
@CapabilityDescription("Implementation of ElasticsearchClientService for ElasticSearch 7.x. Note that although " +
        "Elasticsearch 7.x still accepts type information, this implementation will ignore any type usage and " +
        "will only work at the index level to be already compliant with the ElasticSearch 8.x version that will " +
        "completely remove type usage.")
public class Elasticsearch_7_x_ClientService extends AbstractControllerService implements ElasticsearchClientService {


    protected volatile transient RestHighLevelClient esClient;
    private volatile HttpHost[] esHosts;
    private volatile String authToken;
    protected volatile transient BulkProcessor bulkProcessor;
    private String geolocationFieldLabel;

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
        props.add(ENABLE_SSL);
        props.add(PROP_SHIELD_LOCATION);
        props.add(HOSTS);
        props.add(PROP_SSL_CONTEXT_SERVICE);
        props.add(CHARSET);
        props.add(GEOLOCATION_FIELD_LABEL);

        return Collections.unmodifiableList(props);
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException  {
        super.init(context);
        synchronized(this) {
            try {
                shutdown();
                createElasticsearchClient(context);
                createBulkProcessor(context);
            } catch (Exception e){
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
        try {
            final String username = context.getPropertyValue(USERNAME).asString();
            final String password = context.getPropertyValue(PASSWORD).asString();
            final String hosts = context.getPropertyValue(HOSTS).asString();
            final boolean enableSsl = context.getPropertyValue(ENABLE_SSL).asBoolean();
            geolocationFieldLabel = context.getPropertyValue(GEOLOCATION_FIELD_LABEL).asString();

            esHosts = getEsHosts(hosts, enableSsl);

            if (esHosts != null) {

                RestClientBuilder builder = RestClient.builder(esHosts);

                /**
                 * Inspired from:
                 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_encrypted_communication.html
                 * https://github.com/opendistro-for-elasticsearch/community/issues/64
                 */

                if ((!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) || enableSsl) {

                    builder.setHttpClientConfigCallback(httpClientBuilder -> {

                        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
                            // Support user/password basic auth
                            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                        if (enableSsl) {
                            // Support SSL (ES Shield or OpenDistro)

                            /**
                             * TODO: This current implementation does not verify the server certificate. One could
                             * improve this and provide support for a local truststore to check the server certificate.
                             * The same way, authentication to the server through local certificate is not supported
                             * yet.
                             */

                            // Create and use a trust manager accepting all server certificates
                            TrustManager[] acceptAllTrustManager = new TrustManager[] { new X509TrustManager() {
                                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                                    return null;
                                }
                                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                                }

                                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                                }
                            } };

                            SSLContext sslContext = null;
                            try {
                                sslContext = SSLContext.getInstance("SSL");
                                sslContext.init(null, acceptAllTrustManager, new java.security.SecureRandom());
                            } catch (Exception e) {
                                getLogger().error("Failed to create Elasticsearch client SSLContext due to {}",
                                        new Object[]{e}, e);
                                throw new RuntimeException(e);
                            }

                            httpClientBuilder.setSSLContext(sslContext);
                        }
                        return httpClientBuilder;
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
     * @param enableSsl Enable ssl or not
     * @return List of HttpHost for the ES hosts
     */
    private HttpHost[]  getEsHosts(String hosts, boolean enableSsl) {

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

            esHosts[indHost] = new HttpHost(hostName, port, enableSsl ? "https" : "http");
            indHost++;
        }
        return esHosts;
    }


    protected void createBulkProcessor(ControllerServiceInitializationContext context)
    {
       BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                getLogger().debug("Going to execute bulk [id:{}] composed of {} actions", new Object[]{l, bulkRequest.numberOfActions()});
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                getLogger().debug("Executed bulk [id:{}] composed of {} actions", new Object[]{l, bulkRequest.numberOfActions()});
                if (bulkResponse.hasFailures()) {
                    getLogger().error("There were failures while executing bulk [id:{}]," +
                                    " done bulk request in {} ms with failure = {}",
                            new Object[]{l, bulkResponse.getTook().getMillis(), bulkResponse.buildFailureMessage()});
                    // For each failed doc retrieve the doc we tried to send and display original content
                    // with error returned by the bulkResponse
                    List<DocWriteRequest<?>> requests = bulkRequest.requests();
                    for (BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            String itemId = item.getId();
                            String failureMessage = item.getFailureMessage();
                            for (DocWriteRequest<?> dwr : requests) {
                                if (dwr.id().equals(itemId)) {
                                    getLogger().error("\nBULK ID: " + l +
                                            "\nDOCUMENT ID: " + itemId +
                                            "\nERROR: " + failureMessage +
                                            "\nORIGINAL DOCUMENT:" + dwr.toString() + "\n");
                                }
                            }
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

        // Note: we do not support type anymore but keep it in API (method signature) for backward compatibility
        // purpose. So the type is ignored, even if filled.

        // add it to the bulk,
        IndexRequest request = new IndexRequest(docIndex)
                .source(document, XContentType.JSON)
                .opType(IndexRequest.OpType.INDEX);

        if(OptionalId.isPresent()){
            request.id(OptionalId.get());
        }

        bulkProcessor.add(request);
    }

    @Override
    public void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> OptionalId) {
        // Note: we do not support type anymore but keep it in API (method signature) for backward compatibility
        // purpose. So the type is ignored, even if filled.

        // add it to the bulk
        IndexRequest request = new IndexRequest(docIndex)
                .source(document)
                .opType(IndexRequest.OpType.INDEX);

        if (OptionalId.isPresent()) {
            request.id(OptionalId.get());
        }

        bulkProcessor.add(request);
    }

    @Override
    public void bulkDelete(String docIndex, String docType, String id) {
        DeleteRequest request = new DeleteRequest(docIndex, id);
        bulkProcessor.add(request);
    }


    @Override
    public void deleteByQuery(QueryRecord queryRecord) throws DatastoreClientServiceException {
        String[] indices = new String[queryRecord.getCollections().size()];
        DeleteByQueryRequest request = new DeleteByQueryRequest(queryRecord.getCollections().toArray(indices));
        QueryBuilder builder = toQueryBuilder(queryRecord);
        request.setQuery(builder);
        if (queryRecord.getSize() >= 0) {
            request.setSize(queryRecord.getSize());
        }
        //TODO supporting sort, useful only when using size. Well not needed at the moment
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

    @Override
    public QueryResponseRecord queryGet(QueryRecord queryRecord) throws DatastoreClientServiceException {
        final SearchRequest searchRequest = buildSearchRequest(queryRecord);
        getLogger().info("Elasticsearch queryGet:");
        getLogger().info(searchRequest.toString());
        getLogger().info(searchRequest.getDescription());
        try {
            SearchResponse searchRsp = esClient.search(searchRequest, RequestOptions.DEFAULT);
            return buildQueryResponseRecord(searchRsp);
        } catch (IOException e) {
            getLogger().error("error while queryGet", e);
            throw new DatastoreClientServiceException(e);
        }
    }

    public QueryResponseRecord buildQueryResponseRecord(SearchResponse searchRsp) {
        Objects.requireNonNull(searchRsp);
        if (getLogger().isTraceEnabled()) {
            getLogger().trace("response was {}", new Object[]{searchRsp});
        }
        long totalMatched = -1;
        List<ResponseRecord> docs = new ArrayList<>();
        if (searchRsp.getHits() != null &&
                searchRsp.getHits().getHits() != null &&
                searchRsp.getHits().getTotalHits() != null) {
            totalMatched = searchRsp.getHits().getTotalHits().value;
            long totalReturned = searchRsp.getHits().getHits().length;
            getLogger().info("Number of documents returned is {}, Total number of documents that matched is {}.",
                    new Object[]{
                            totalReturned,
                            totalMatched
                    });
            for (SearchHit hit : searchRsp.getHits().getHits()) {
                Map<String,Object> responseMap = hit.getSourceAsMap();
                docs.add(new ResponseRecord(hit.getIndex(), hit.getType(), hit.getId(), responseMap));
            }
        }
        List<AggregationResponseRecord> aggregationResponseRecords = new ArrayList<>();
        Aggregations aggregations = searchRsp.getAggregations();
        if (aggregations != null && !aggregations.asList().isEmpty()) {
            aggregations.asList().forEach(aggregation -> aggregationResponseRecords.add(buildAggregationResponseRecord(aggregation)));
        }
        return new QueryResponseRecord(totalMatched, docs, aggregationResponseRecords);
    }

    AggregationResponseRecord buildAggregationResponseRecord(Aggregation aggregation) {
        String aggName = aggregation.getName();
        String aggType = aggregation.getType();
        if (aggregation.getMetaData() == null) {
            getLogger().debug("aggregation metadata: null");
        } else {
            getLogger().debug("aggregation metadata:");
            for (Map.Entry<String, Object> metadataEntry : aggregation.getMetaData().entrySet()) {
                if (metadataEntry != null) {
                    getLogger().debug(metadataEntry.getKey() + " = " + metadataEntry.getValue());
                }
            }
        }
        if (aggregation instanceof ParsedMin) {
            ParsedMin parsedMin = (ParsedMin) aggregation;
            getLogger().debug("Min agg result : \n" + parsedMin.getValue() + "\n" + parsedMin.getValueAsString());
            if (Double.isInfinite(parsedMin.getValue())) {
                return new MinAggregationResponseRecord(aggName, aggType, Long.MAX_VALUE);
            }
            long min = (long) parsedMin.getValue(); // this is a timestamp
            return new MinAggregationResponseRecord(aggName, aggType, min);
        } else {
            throw new NotImplementedException("Unsupported aggregation class " + aggregation.getClass().getName());
        }

    }

    public SearchRequest buildSearchRequest(QueryRecord queryRecord) {
        //build SearchSourceBuilder
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        final QueryBuilder queryBuilder = toQueryBuilder(queryRecord);
        searchSourceBuilder.query(queryBuilder);
        if (queryRecord.getSize() >= 0) {
            searchSourceBuilder.size(queryRecord.getSize());
        }
        queryRecord.getAggregationQueries().forEach(aggregationRecord -> searchSourceBuilder.aggregation(toAggregationBuilder(aggregationRecord)));
        List<SortBuilder<?>> sortBuilders = toSortBuilders(queryRecord);
        sortBuilders.forEach(searchSourceBuilder::sort);
        //build SearchRequest
        String[] indices = new String[queryRecord.getCollections().size()];
        return new SearchRequest(queryRecord.getCollections().toArray(indices), searchSourceBuilder);
    }

    private List<SortBuilder<?>> toSortBuilders(QueryRecord queryRecord) {
        List<SortBuilder<?>> sortBuilders = new ArrayList<>();
        for (SortQueryRecord sortQuery : queryRecord.getSortQueries()) {
            FieldSortBuilder sortBuilder = SortBuilders.fieldSort(sortQuery.getFieldName());
            switch (sortQuery.getSortingOrder()) {
                case ASC:
                    sortBuilder = sortBuilder.order(SortOrder.ASC);
                    break;
                case DESC:
                    sortBuilder = sortBuilder.order(SortOrder.DESC);
                    break;
            }
            sortBuilders.add(sortBuilder);
        }
        return sortBuilders;
    }

    private QueryBuilder toQueryBuilder(BoolQueryRecord boolQueryRecord) {
        if (boolQueryRecord instanceof BoolQueryRecordRoot) {
            return toQueryBuilder((BoolQueryRecordRoot) boolQueryRecord);
        } else if (boolQueryRecord instanceof RangeQueryRecord) {
            RangeQueryRecord rangeQuery = (RangeQueryRecord) boolQueryRecord;
            return QueryBuilders
                    .rangeQuery(rangeQuery.getFieldName())
                    .from(rangeQuery.getFrom(), rangeQuery.isIncludeLower())
                    .to(rangeQuery.getTo(), rangeQuery.isIncludeUpper());
        } else if (boolQueryRecord instanceof TermQueryRecord) {
            TermQueryRecord termQuery = (TermQueryRecord) boolQueryRecord;
            return QueryBuilders.termQuery(termQuery.getFieldName(), termQuery.getFieldValue());
        } else if (boolQueryRecord instanceof WildCardQueryRecord) {
            WildCardQueryRecord wildCardQuery = (WildCardQueryRecord) boolQueryRecord;
            return QueryBuilders.wildcardQuery(wildCardQuery.getFieldName(), wildCardQuery.getFieldValue());
        } else {
            getLogger().error("BoolQueryRecord of class " + boolQueryRecord.getClass() + " is not yet supported");
            throw new IllegalArgumentException("BoolQueryRecord of class " + boolQueryRecord.getClass() + " is not yet supported");
        }
    }

    private QueryBuilder toQueryBuilder(BoolQueryRecordRoot boolQueryRoot) {
        final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQueryRoot.getChildren().forEach(node -> {
            QueryBuilder query = toQueryBuilder(node.getData());
            switch (node.getBoolCondition()) {
                case MUSTNOT:
                    boolQuery.mustNot(query);
                    break;
                case MUST:
                    boolQuery.must(query);
                    break;
                case SHOULD:
                    boolQuery.should(query);
                    break;
            }
        });
        return boolQuery;
    }

    private QueryBuilder toQueryBuilder(QueryRecord queryRecord) {
        return toQueryBuilder(queryRecord.getBoolQuery());
    }

    private static AggregationBuilder toAggregationBuilder(AggregationRecord aggregationRecord) {
        AggregationBuilder aggregationBuilder = null;
        String aggName = aggregationRecord.getAggregationName();
        String aggField = aggregationRecord.getFieldName();
        if (aggregationRecord instanceof MinAggregationRecord) {
            aggregationBuilder = AggregationBuilders.min(aggName).field(aggField);
        } else {
            throw new NotImplementedException("Unsupported AggregationRecord subclass " + aggregationRecord.getClass().getName());
        }
        return aggregationBuilder;
    }

    private static List<AggregationBuilder> toAggregationBuilder(QueryRecord queryRecord) {
        List<AggregationBuilder> aggregationBuildersList = new ArrayList<>();
        return queryRecord.getAggregationQueries().stream().map(Elasticsearch_7_x_ClientService::toAggregationBuilder).collect(Collectors.toList());
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
            RefreshResponse rsp = esClient.indices().refresh(request, RequestOptions.DEFAULT);
            getLogger().info("refresh response for indices {} is {}", new Object[]{indices, rsp.getStatus()});
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
            List<String> documentIds = multiGetQueryRecord.getDocumentIds();
            String[] fieldsToInclude = multiGetQueryRecord.getFieldsToInclude();
            String[] fieldsToExclude = multiGetQueryRecord.getFieldsToExclude();
            if ((fieldsToInclude != null && fieldsToInclude.length > 0) || (fieldsToExclude != null && fieldsToExclude.length > 0)) {
                for (String documentId : documentIds) {
                    MultiGetRequest.Item item = new MultiGetRequest.Item(index, documentId);
                    item.fetchSourceContext(new FetchSourceContext(true, fieldsToInclude, fieldsToExclude));
                    multiGetRequest.add(item);
                }
            } else {
                for (String documentId : documentIds) {
                    multiGetRequest.add(index, documentId);
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
    public MultiQueryResponseRecord multiQueryGet(MultiQueryRecord queryRecords) throws DatastoreClientServiceException{
        final MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        List<SearchRequest> searchRequests = buildSearchRequests(queryRecords);
        searchRequests.forEach(multiSearchRequest::add);
        try {
            MultiSearchResponse multiSearchResponse = esClient.msearch(multiSearchRequest, RequestOptions.DEFAULT);
            if (getLogger().isTraceEnabled()) {
                getLogger().trace("response was {}", new Object[]{multiSearchResponse});
            }
            List<QueryResponseRecord> searchResponses = new ArrayList<>();

            if (multiSearchResponse.getResponses() != null) {
                MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
                for (MultiSearchResponse.Item item : items) {
                    SearchResponse searchResponse = item.getResponse();
                    if (searchResponse != null) {
                        searchResponses.add(buildQueryResponseRecord(searchResponse));
                    } else {
                        if (item.isFailure()) {
                            getLogger().error("a search request failed because :\n'{}'", new Object[]{item.getFailureMessage()});
                        }
                    }
                }
            }

            return new MultiQueryResponseRecord(searchResponses);
        } catch (IOException e) {
            getLogger().error("error while queryGet", e);
            throw new DatastoreClientServiceException(e);
        }
    }

    private List<SearchRequest> buildSearchRequests(MultiQueryRecord queryRecords) {
        return queryRecords.getQueries().stream()
                .map(this::buildSearchRequest)
                .collect(Collectors.toList());
    }

    @Override
    public boolean existsCollection(String indexName) throws DatastoreClientServiceException {
        boolean exists;
        try {
            GetIndexRequest request = new GetIndexRequest(indexName);
            exists = esClient.indices().exists(request, RequestOptions.DEFAULT);
        }
        catch (Exception e){
            throw new DatastoreClientServiceException(e);
        }
        return exists;
    }



    @Override
    public void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception {
        IndexRequest indexRequest = new IndexRequest(indexName).source(doc);
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
            ReindexRequest request = new ReindexRequest().setSourceIndices(srcIndex).setDestIndex(dstIndex);
            esClient.reindex(request, RequestOptions.DEFAULT);
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
        request.source(mappingAsJsonString, XContentType.JSON);

        try {
            AcknowledgedResponse rsp = esClient.indices().putMapping(request, RequestOptions.DEFAULT);
            if (!rsp.isAcknowledged()) {
                throw new DatastoreClientServiceException("Elasticsearch mapping definition not acknowledged");
            }
            return true;
        } catch (Exception e) {
            getLogger().error("Failed to put ES mapping for index {} : {}", new Object[]{indexName, e});
            // This is an error that can be fixed by providing alternative inputs so return boolean rather
            // than throwing an exception.
            return false;
        }
    }

    @Override
    public void bulkPut(String indexName, Record record) throws DatastoreClientServiceException {

        // Note: we do not support type anymore but keep it in API (method signature) for backward compatibility
        // purpose. So the type is ignored, even if filled. So we check for presence of the ',' separator.
        // If present, a type was provided and we ignore it, if not present, the whole string is used as the
        // index name.

        if (indexName.contains(","))
        {
            final List<String> indexType = Arrays.asList(indexName.split(","));
            if (indexType.size() == 2) {
                indexName = indexType.get(0);
                // Ignore type which is at index 1
            }
            else {
                throw new DatastoreClientServiceException("Could not parse index/type name although the provided " +
                        "string contains a ',' separator: " + indexName);
            }
        } else {

        }

        bulkPut(indexName, null, convertRecordToString(record), Optional.of(record.getId()));
    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for ElasticSearch 7.x");
    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for ElasticSearch 7.x");
    }

    @Override
    public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for ElasticSearch 7.x");
    }

    @Override
    public Collection<Record> query(String query) {
        throw new NotImplementedException("Not yet supported for ElasticSearch 7.x");
    }

    @Override
    public long queryCount(String query) {
        throw new NotImplementedException("Not yet supported for ElasticSearch 7.x");
    }

    @Override
    public String convertRecordToString(Record record) {
        if (geolocationFieldLabel != null) {
          return ElasticsearchRecordConverter.convertToString(record, geolocationFieldLabel);
        }
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
            sourceBuilder.explain(false);

            searchRequest.source(sourceBuilder);

            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            numberOfHits = searchResponse.getHits().getTotalHits().value;
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