package com.hurence.logisland.processor.elasticsearchasaservice;

import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hurence.logisland.processor.elasticsearchasaservice.AbstractElasticsearchProcessor.TODAY_DATE_SUFFIX;


public class TestPutElasticsearch {

    private volatile Client esClient;
    private volatile BulkProcessor bulkProcessor;
    private volatile Map<String/*id*/, String/*errors*/> errors = new HashMap<>();

    private static Logger logger = LoggerFactory.getLogger(TestPutElasticsearch.class);

    @Rule
    public final ESRule esRule = new ESRule();


    private class MockElasticsearchClientService extends AbstractControllerService implements ElasticsearchClientService {

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

        protected void createElasticsearchClient(ControllerServiceInitializationContext context) throws ProcessException {
            if (esClient != null) {
                return;
            }
            esClient = esRule.getClient();
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
                    .setBulkActions(1000)
                    .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(1))
                    .setConcurrentRequests(2)
                    //.setBackoffPolicy(getBackOffPolicy(context))
                    .build();
        }

        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

            List<PropertyDescriptor> props = new ArrayList<>();

            return Collections.unmodifiableList(props);
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

    @Test
    public void testSingleRecord() throws IOException, InitializationException {

        final String DEFAULT_INDEX = "test_index";
        final String DEFAULT_TYPE = "cisco_record";
        final String ES_INDEX_FIELD = "index_field";
        final String ES_TYPE_FIELD = "type_field";

        //////////////////
        final TestRunner runner = TestRunners.newTestRunner(PutElasticsearch.class);
        runner.setProperty(PutElasticsearch.DEFAULT_INDEX, DEFAULT_INDEX);
        runner.setProperty(PutElasticsearch.DEFAULT_TYPE, DEFAULT_TYPE);
        runner.setProperty(PutElasticsearch.TIMEBASED_INDEX, TODAY_DATE_SUFFIX);
        runner.setProperty(PutElasticsearch.ES_INDEX_FIELD, ES_INDEX_FIELD);
        runner.setProperty(PutElasticsearch.ES_TYPE_FIELD, ES_TYPE_FIELD);
        runner.setProperty(PutElasticsearch.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");

        runner.assertValid();

        ///////////////////
        final MockElasticsearchClientService elasticsearchClient = new MockElasticsearchClientService();
        runner.addControllerService("elasticsearchClient", elasticsearchClient);
        runner.enableControllerService(elasticsearchClient);

        ///////////////////
        final Record inputRecord1 = new StandardRecord(DEFAULT_TYPE)
                .setId("firewall_record0")
                .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                .setField("method", FieldType.STRING, "GET")
                .setField("ip_source", FieldType.STRING, "123.34.45.123")
                .setField("ip_target", FieldType.STRING, "255.255.255.255")
                .setField("url_scheme", FieldType.STRING, "http")
                .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                .setField("url_port", FieldType.STRING, "80")
                .setField("url_path", FieldType.STRING, "/r15lgc-100KB.js")
                .setField("request_size", FieldType.INT, 1399)
                .setField("response_size", FieldType.INT, 452)
                .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")));

        final Record inputRecord2 = new StandardRecord(DEFAULT_TYPE)
                .setId("firewall_record1")
                .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                .setField("method", FieldType.STRING, "GET")
                .setField("ip_source", FieldType.STRING, "123.34.45.12")
                .setField("ip_target", FieldType.STRING, "255.255.255.255")
                .setField("url_scheme", FieldType.STRING, "http")
                .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                .setField("url_port", FieldType.STRING, "80")
                .setField("url_path", FieldType.STRING, 45)
                .setField("request_size", FieldType.INT, 1399)
                .setField("response_size", FieldType.INT, 452)
                .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")));

        runner.enqueue(inputRecord1);
        runner.enqueue(inputRecord2);
        runner.clearQueues();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(2);
        runner.assertOutputErrorCount(0);
        elasticsearchClient.flushBulkProcessor();

        try {
            Thread.sleep(1000);
            elasticsearchClient.refreshIndex(DEFAULT_INDEX);
            Assert.assertEquals(2, elasticsearchClient.countIndex(DEFAULT_INDEX));
        } catch (Exception e) {
            e.printStackTrace();
        }

        long numberOfHits = elasticsearchClient.searchNumberOfHits(DEFAULT_INDEX, DEFAULT_TYPE, "ip_source", "123.34.45.123");

        Assert.assertEquals(1,numberOfHits);

    }

}