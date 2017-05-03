/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.util.elasticsearch;


import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility methods for Elasticsearch.
 */
public final class ElasticsearchUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchUtils.class);

    private static final String ES_SETTINGS_KEY_NSHARDS = "number_of_shards";
    private static final String ES_SETTINGS_KEY_NREPLICAS = "number_of_replicas";

    private ElasticsearchUtils() {
    }

    /**
     * Return true if the specified index exists (also true if the name is an alias to an index).
     */
    public static boolean existsIndex(Client client, String indexName) throws IOException {
        // could also use  client.admin().indices().prepareExists(indexName).execute().get();
        IndicesExistsResponse ersp = IndicesExistsAction.INSTANCE.newRequestBuilder(client).setIndices(indexName).get();
        return ersp.isExists();
    }

    /**
     * Wait until the specified index has integrated all previously-saved data.
     */
    public static void refreshIndex(Client client, String indexName) throws Exception {
        client.admin().indices().prepareRefresh(indexName).execute().get();
    }

    /**
     * Save the specified object to the index.
     */
    public static void saveAsync(Client client, String indexName, String doctype, Map<String, Object> doc) throws Exception {
        client.prepareIndex(indexName, doctype).setSource(doc).execute().get();
    }

    /**
     * Save the specified object to the index.
     */
    public static void saveSync(Client client, String indexName, String doctype, Map<String, Object> doc) throws Exception {
        client.prepareIndex(indexName, doctype).setSource(doc).execute().get();
        refreshIndex(client, indexName);
    }

    /**
     * Return the number of documents in the index.
     */
    public static long countIndex(Client client, String indexName) throws Exception {
        // There is no "count" method; instead make a "search for all" and set the desired number of returned
        // records to zero. No actual hits get returned, but the "metadata" for the result includes the total
        // number of matched records, ie the size.
        SearchResponse rsp = client.prepareSearch(indexName)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(0)
                .execute().get();
        return rsp.getHits().getTotalHits();
    }

    /**
     * Delete the specified index.
     */
    public static void createIndex(Client client, int numShards, int numReplicas, String indexName) throws IOException {
        // Define the index itself
        CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate(indexName);
        builder.setSettings(Settings.builder()
                .put(ES_SETTINGS_KEY_NSHARDS, numShards)
                .put(ES_SETTINGS_KEY_NREPLICAS, numReplicas)
                .build());

        try {
            CreateIndexResponse rsp = builder.execute().get();
            if (!rsp.isAcknowledged()) {
                throw new IOException("Elasticsearch index definition not acknowledged");
            }
            LOG.info("Created index {}", indexName);
        } catch (Exception e) {
            LOG.error("Failed to create ES index", e);
            throw new IOException("Failed to create ES index", e);
        }
    }

    /**
     * Delete the specified index.
     */
    public static void dropIndex(Client client, String indexName) throws IOException {
        try {
            client.admin().indices().prepareDelete(indexName).execute().get();
            LOG.info("Delete index {}", indexName);
        } catch (Exception e) {
            throw new IOException(String.format("Unable to delete index %s", indexName), e);
        }
    }

    /**
     * Copy the contents of srcIndex into dstIndex.
     * <p>
     * Although ES provides a "reindex" REST endpoint, it does so via a "standard extension module" rather than
     * implementing the logic in ES core itself. This means there is no reindex java API; we must implement
     * reindexing as a search-scroll loop.
     * </p>
     * <p>
     * Credits: http://blog.davidvassallo.me/2016/10/11/elasticsearch-java-tips-for-faster-re-indexing/
     * </p>
     */
    public static void copyIndex(Client client, TimeValue reindexScrollTimeout, String srcIndex, String dstIndex)
            throws IOException {

        SearchResponse scrollResp = client.prepareSearch(srcIndex)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setScroll(reindexScrollTimeout)
                .setQuery(QueryBuilders.matchAllQuery()) // Match all query
                .setSize(100) // 100 hits per shard will be returned for each scroll
                .execute().actionGet();

        AtomicBoolean failed = new AtomicBoolean(false);

        // A BulkProcessor listener is invoked from a thread-pool, so must be threadsafe..
        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                LOG.debug("Executing bulk composed of {} actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                LOG.debug("Executed bulk composed of {} actions", request.numberOfActions());
                if (response.hasFailures()) {
                    failed.set(true);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                LOG.warn("Error executing bulk", failure);
                failed.set(true);
            }
        });

        // A user of a BulkProcessor just keeps adding requests to it, and the BulkProcessor itself decides when
        // to send a request to the ES nodes, based on its configuration settings. Calls can be triggerd by number
        // of queued requests, total size of queued requests, and time since previous request. The defaults for
        // these settings are all sensible, so are not overridden here. The BulkProcessor has an internal threadpool
        // which allows it to send multiple batches concurrently; the default is "1" meaning that a single completed
        // batch can be sending in the background while a new batch is being built. When the non-active batch is
        // "full", the add call blocks until the background batch completes.
        BulkProcessor bulkProcessor = bulkProcessorBuilder.build();
        try {
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
                scrollResp = client.prepareSearchScroll(scrollId)
                        .setScroll(reindexScrollTimeout)
                        .execute().actionGet();
            }
        } finally {
            // BulkProcessor implements Closeable but not Autocloseable, so cannot be used in try-with-resources.
            boolean done;
            try {
                done = bulkProcessor.awaitClose(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                done = false;
            }
            if (!done) {
                throw new IOException("Data could not be flushed to target index");
            }
        }

        if (failed.get()) {
            throw new IOException("Reindex failed to insert one or more documents");
        }

        LOG.info("Reindex completed");
    }

    /**
     * Creates an alias.
     */
    public static void createAlias(Client client, String indexName, String aliasName) throws IOException {
        IndicesAliasesRequestBuilder builder = client.admin().indices().prepareAliases().addAlias(indexName, aliasName);
        try {
            IndicesAliasesResponse rsp = builder.execute().get();
            if (!rsp.isAcknowledged()) {
                throw new IOException(String.format(
                        "Creation of elasticsearch alias '%s' for index '%s' not acknowledged.", aliasName, indexName));
            }
        } catch (IOException e) {
            LOG.error("Failed to create elasticsearch alias {} for index {}", aliasName, indexName, e);
            throw e;
        } catch (ExecutionException | InterruptedException e) {
            String msg = String.format("Failed to create elasticsearch alias '%s' for index '%s'", aliasName, indexName);
            throw new IOException(msg, e);
        }
    }

    /**
     * Adds a mapping to an index, or overwrites an existing mapping.
     * <p>
     * If the new mapping is "not compatible" with the index, then false is returned. If a system-error occurred
     * while updating the index, an exception is thrown.
     * </p>
     */
    public static boolean putMapping(Client client, String indexName, String doctype, String mappingAsJsonString)
            throws IOException {

        PutMappingRequestBuilder builder = client.admin().indices().preparePutMapping(indexName);
        builder.setType(doctype).setSource(mappingAsJsonString);

        try {
            PutMappingResponse rsp = builder.execute().get();
            if (!rsp.isAcknowledged()) {
                throw new IOException("Elasticsearch mapping definition not acknowledged");
            }
            return true;
        } catch (Exception e) {
            LOG.error("Failed to load ES mapping {} for index {}", doctype, indexName, e);
            // This is an error that can be fixed by providing alternative inputs so return boolean rather
            // than throwing an exception.
            return false;
        }
    }
}
