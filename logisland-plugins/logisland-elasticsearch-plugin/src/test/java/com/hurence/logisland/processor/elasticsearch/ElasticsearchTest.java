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

import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.elasticsearch.ElasticsearchUtils;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElasticsearchTest {

    private static final String MAPPING1 = "{'properties':{'name':{'type':'keyword'},'val':{'type':'integer'}}}";
    private static final String MAPPING2 = "{'properties':{'name':{'type':'keyword'},'val':{'type':'keyword'}}}";
    private static final String MAPPING3 =
            "{'dynamic':'strict','properties':{'name':{'type':'keyword'},'xyz':{'type':'keyword'}}}";

    @Rule
    public final ESRule esRule = new ESRule();

    @Test
    public void testBasics() throws Exception {
        Client client = esRule.getClient();

        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("name", "fred");
        doc1.put("val", 33);

        boolean result;

        // Verify the index does not exist
        Assert.assertEquals(false, ElasticsearchUtils.existsIndex(client, "foo"));

        // Define the index
        ElasticsearchUtils.createIndex(client, 2, 1, "foo");
        Assert.assertEquals(true, ElasticsearchUtils.existsIndex(client, "foo"));

        // Define another index
        ElasticsearchUtils.createIndex(client, 2, 1, "bar");
        Assert.assertEquals(true, ElasticsearchUtils.existsIndex(client, "foo"));

        // Add a mapping to foo
        result = ElasticsearchUtils.putMapping(client, "foo", "type1", MAPPING1.replace('\'', '"'));
//        Assert.assertEquals(true, result);

        // Add the same mapping again
        result = ElasticsearchUtils.putMapping(client, "foo", "type1", MAPPING1.replace('\'', '"'));
        //       Assert.assertEquals(true, result);

        // Add the same mapping again under a different doctype
        result = ElasticsearchUtils.putMapping(client, "foo", "type2", MAPPING1.replace('\'', '"'));
        //       Assert.assertEquals(true, result);

        // Update a mapping with an incompatible mapping -- should fail
        result = ElasticsearchUtils.putMapping(client, "foo", "type2", MAPPING2.replace('\'', '"'));
        Assert.assertEquals(false, result);

        // create alias
        ElasticsearchUtils.createAlias(client, "foo", "aliasFoo");
        Assert.assertEquals(true, ElasticsearchUtils.existsIndex(client, "aliasFoo"));

        // Insert a record into foo and count foo
        Assert.assertEquals(0, ElasticsearchUtils.countIndex(client, "foo"));
        ElasticsearchUtils.saveSync(client, "foo", "type1", doc1);
        Assert.assertEquals(1, ElasticsearchUtils.countIndex(client, "foo"));

        // copy index foo to bar - should work
        Assert.assertEquals(0, ElasticsearchUtils.countIndex(client, "bar"));
        ElasticsearchUtils.copyIndex(client, TimeValue.timeValueMinutes(2), "foo", "bar");
        ElasticsearchUtils.refreshIndex(client, "bar");
        Assert.assertEquals(1, ElasticsearchUtils.countIndex(client, "bar"));

        // Define incompatible mappings for the same doctype in two different indexes, then try to copy - should fail
        // as a document registered with doctype=type1 in index foo cannot be written as doctype=type1 in index baz.
        //
        // Note: MAPPING2 cannot be added to index foo or bar at all, even under a different doctype, as ES (lucene)
        // does not allow two types for the same field-name in different mappings of the same index. However if
        // MAPPING2 is added to index baz, then the copyIndex succeeds - because by default ES automatically converts
        // integers into strings when necessary. Interestingly, this means MAPPING1 and MAPPING2 are not compatible
        // at the "put mapping" level, but are compatible at the "reindex" level..
        //
        // The document (doc1) of type "type1" already in index "foo" cannot be inserted into index "baz" as type1
        // because that means applying its source to MAPPING3 - but MAPPING3 is strict and does not define property
        // "val", so the insert fails.
        ElasticsearchUtils.createIndex(client, 2, 1, "baz");
        ElasticsearchUtils.putMapping(client, "baz", "type1", MAPPING3.replace('\'', '"'));

      /*  try {
            ElasticsearchUtils.copyIndex(client, TimeValue.timeValueMinutes(2), "foo", "baz");
            Assert.fail("Exception not thrown when expected");
        } catch(IOException e) {
            Assert.assertTrue(e.getMessage().contains("Reindex failed"));
        }*/
        ElasticsearchUtils.refreshIndex(client, "baz");
        Assert.assertEquals(0, ElasticsearchUtils.countIndex(client, "baz"));

        // Drop index foo
        ElasticsearchUtils.dropIndex(client, "foo");
        Assert.assertEquals(false, ElasticsearchUtils.existsIndex(client, "foo"));
        Assert.assertEquals(false, ElasticsearchUtils.existsIndex(client, "aliasFoo")); // alias for foo disappears too
        Assert.assertEquals(true, ElasticsearchUtils.existsIndex(client, "bar"));
    }


    @Test
    public void testIndexation() throws Exception {
        Client client = esRule.getClient();

        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("name", "fred");
        doc1.put("val", 33);


        // Verify the index does not exist
        Assert.assertEquals(false, ElasticsearchUtils.existsIndex(client, "foo"));

        // Define the index
        ElasticsearchUtils.createIndex(client, 2, 1, "foo");
        Assert.assertEquals(true, ElasticsearchUtils.existsIndex(client, "foo"));



        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long l, BulkRequest bulkRequest) {

                    }

                    @Override
                    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

                    }

                    @Override
                    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

                    }

                })
                .setBulkActions(10)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(2))
                .setConcurrentRequests(2)
                .setBackoffPolicy(BackoffPolicy.noBackoff())
                .build();

        // add it to the bulk
        IndexRequestBuilder result = client
                .prepareIndex("foo", "type1")
                .setId("id1")
                .setSource(doc1)
                .setOpType(IndexRequest.OpType.INDEX);
        bulkProcessor.add(result.request());

        bulkProcessor.flush();


        ElasticsearchUtils.refreshIndex(client, "foo");


        // Insert a record into foo and count foo
        Assert.assertEquals(0, ElasticsearchUtils.countIndex(client, "foo"));
        ElasticsearchUtils.saveSync(client, "foo", "type1", doc1);
        ElasticsearchUtils.refreshIndex(client, "foo");
        Assert.assertEquals(2, ElasticsearchUtils.countIndex(client, "foo"));


        SearchResponse searchResponse = client.prepareSearch("foo")
                .setTypes("type1")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("name", "fred"))
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();

        Assert.assertEquals(2, searchResponse.getHits().getTotalHits());

    }

    @Test
    public void validatePutES() throws Exception {
        Client client = esRule.getClient();
        final String indexName = "test";
        final String recordType = "cisco_record";
        final TestRunner testRunner = TestRunners.newTestRunner(new PutElasticsearch());
        testRunner.setProperty("hosts", "local[0]:9300"); // not used because of Client injection
        testRunner.setProperty("default.type", recordType);
        testRunner.setProperty("cluster.name", EmbeddedElasticsearchServer.CLUSTER_NAME);
        testRunner.setProperty("default.index", indexName);
        testRunner.setProperty(PutElasticsearch.BATCH_SIZE, "2");
        testRunner.assertValid();


        Record[] records = {
                new StandardRecord(recordType)
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
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail"))),
                new StandardRecord(recordType)
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
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")))
        };

        PutElasticsearch processor = (PutElasticsearch) testRunner.getProcessContext().getProcessor();
        processor.setClient(client);

        testRunner.enqueue(records);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.assertOutputErrorCount(0);


        ElasticsearchUtils.refreshIndex(client, indexName);

        Assert.assertEquals(2, ElasticsearchUtils.countIndex(client, indexName));
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(recordType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("ip_source", "123.34.45.123"))
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();
        Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
    }


    /**
     * verify that retry parameter is working correctly
     * @throws Exception
     */
    @Test
    public void retryPutES() throws Exception {

        Client client = esRule.getClient();
        final String indexName = "test";
        final String recordType = "cisco_record";
        final TestRunner testRunner = TestRunners.newTestRunner(new PutElasticsearch());
        testRunner.setProperty("hosts", "local[1]:9300");
        testRunner.setProperty("default.type", recordType);
        testRunner.setProperty("cluster.name", EmbeddedElasticsearchServer.CLUSTER_NAME);
        testRunner.setProperty("default.index", indexName);
        testRunner.setProperty("batch.size", "2");
        testRunner.setProperty("num.retry", "50");
        testRunner.setProperty("throttling.delay", "2000");
        testRunner.setProperty("backoff.policy", "exponentialBackoff");
        testRunner.assertValid();


        Record[] records = {
                new StandardRecord(recordType)
                        .setId("firewall_record1")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "GET")
                        .setField("ip_source", FieldType.STRING, "123.34.45.123")
                        .setField("ip_target", FieldType.STRING, "255.255.255.255")
                        .setField("url_scheme", FieldType.STRING, "http")
                        .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                        .setField("url_port", FieldType.STRING, "80")
                        .setField("url_path", FieldType.STRING, 45)
                        .setField("request_size", FieldType.INT, 1399)
                        .setField("response_size", FieldType.INT, 452)
                        .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                        .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail"))),
                new StandardRecord(recordType)
                        .setId("firewall_record1")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "GET")
                        .setField("ip_source", FieldType.STRING, "123.34.45.12")
                        .setField("ip_target", FieldType.STRING, "255.255.255.255")
                        .setField("url_scheme", FieldType.STRING, "http")
                        .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                        .setField("url_port", FieldType.STRING, "80")
                        .setField("url_path", FieldType.STRING, 8888)
                        .setField("request_size", FieldType.INT, 1399)
                        .setField("response_size", FieldType.INT, 452)
                        .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                        .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail"))),
                new StandardRecord(recordType)
                        .setId("firewall_record1")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "GET")
                        .setField("ip_source", FieldType.STRING, "123.34.45.12")
                        .setField("ip_target", FieldType.STRING, "255.255.255.255")
                        .setField("url_scheme", FieldType.STRING, "http")
                        .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                        .setField("url_port", FieldType.STRING, "80")
                        .setField("url_path", FieldType.STRING, 8888)
                        .setField("request_size", FieldType.INT, 1399)
                        .setField("response_size", FieldType.INT, 452)
                        .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                        .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail"))),
                new StandardRecord(recordType)
                        .setId("firewall_record2")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "GET")
                        .setField("ip_source", FieldType.STRING, "123.34.45.12")
                        .setField("ip_target", FieldType.STRING, "255.255.255.255")
                        .setField("url_scheme", FieldType.STRING, "http")
                        .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                        .setField("url_port", FieldType.STRING, "80")
                        .setField("url_path", FieldType.STRING, 8888)
                        .setField("request_size", FieldType.INT, 1399)
                        .setField("response_size", FieldType.INT, 452)
                        .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                        .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")))
        };


        PutElasticsearch processor = (PutElasticsearch) testRunner.getProcessContext().getProcessor();
        processor.setClient(client);

        testRunner.enqueue(records);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(4);

       /* List<MockRecord> failedRecords = testRunner.getOutputRecords();
        for (MockRecord failedRecord: failedRecords) {
            List<String> errors = (List<String>) failedRecord.getErrors();
            Assert.assertEquals("there should be one error", errors.size(), 1);
            Assert.assertTrue("there should be one error", errors.get(0).contains(ProcessError.DUPLICATE_ID_ERROR.getName()));
        }*/

        ElasticsearchUtils.refreshIndex(client, indexName);


        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(recordType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();


        Assert.assertEquals(2, searchResponse.getHits().getTotalHits());

        Record[] badRecords = {
                new StandardRecord(recordType)
                        .setId("firewall_record1")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "GET")
                        .setField("ip_source", FieldType.STRING, "123.34.45.12")
                        .setField("ip_target", FieldType.STRING, "255.255.255.255")
                        .setField("url_scheme", FieldType.STRING, "http")
                        .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                        .setField("url_port", FieldType.STRING, "80")
                        .setField("url_path", FieldType.INT, 8888)
                        .setField("request_size", FieldType.INT, 1399)
                        .setField("response_size", FieldType.INT, 452)
                        .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                        .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail"))),
                new StandardRecord(recordType)
                        .setId("firewall_record3")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "GET")
                        .setField("ip_source", FieldType.STRING, "123.34.45.12")
                        .setField("ip_target", FieldType.STRING, "255.255.255.255")
                        .setField("url_scheme", FieldType.STRING, "http")
                        .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                        .setField("url_port", FieldType.STRING, "80")
                        .setField("url_path", FieldType.INT, 8888)
                        .setField("request_size", FieldType.INT, 1399)
                        .setField("response_size", FieldType.INT, 452)
                        .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                        .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail"))),
                new StandardRecord(recordType)
                        .setId("firewall_record4")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "GET")
                        .setField("ip_source", FieldType.STRING, "123.34.45.12")
                        .setField("ip_target", FieldType.STRING, "255.255.255.255")
                        .setField("url_scheme", FieldType.STRING, "http")
                        .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                        .setField("url_port", FieldType.STRING, "80")
                        .setField("url_path", FieldType.STRING, "ddddddd")
                        .setField("request_size", FieldType.INT, 1399)
                        .setField("response_size", FieldType.INT, 452)
                        .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                        .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")))
        };

        testRunner.enqueue(badRecords);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);

        /*failedRecords = testRunner.getOutputRecords();
        for (MockRecord failedRecord: failedRecords) {
            List<String> errors = (List<String>) failedRecord.getErrors();
            Assert.assertEquals("there should be one error", errors.size(), 1);
            Assert.assertTrue("there should be one error", errors.get(0).contains(ProcessError.INDEXATION_ERROR.getName()));
        }*/

        ElasticsearchUtils.refreshIndex(client, indexName);
        searchResponse = client.prepareSearch(indexName)
                .setTypes(recordType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();


        Assert.assertEquals(4, searchResponse.getHits().getTotalHits());
    }


    @Test
    public void validateUpdaterecord() throws Exception {
        Client client = esRule.getClient();
        final String indexName = "test";
        final String recordType = "cisco_record";
        final TestRunner testRunner = TestRunners.newTestRunner(new PutElasticsearch());
        testRunner.setProperty("hosts", "local[0]:9300"); // not used because of Client injection
        testRunner.setProperty("default.type", recordType);
        testRunner.setProperty("cluster.name", EmbeddedElasticsearchServer.CLUSTER_NAME);
        testRunner.setProperty("default.index", indexName);
        testRunner.setProperty(PutElasticsearch.BATCH_SIZE, "2");
        testRunner.assertValid();


        Record[] records = {
                new StandardRecord(recordType)
                        .setId("firewall_record0")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "GET")

        };

        PutElasticsearch processor = (PutElasticsearch) testRunner.getProcessContext().getProcessor();
        processor.setClient(client);

        testRunner.enqueue(records);
        testRunner.clearQueues();
        testRunner.run();

        ElasticsearchUtils.refreshIndex(client, indexName);

        Assert.assertEquals(1, ElasticsearchUtils.countIndex(client, indexName));
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(recordType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();
        Assert.assertEquals(1, searchResponse.getHits().getTotalHits());

        String esResponse = searchResponse.getHits().getAt(0).getSourceAsString();
        assertNotNull(esResponse);
        assertTrue(esResponse.contains("GET"));






// update the record

        Record[] records2 = {
                new StandardRecord(recordType)
                        .setId("firewall_record0")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "POST")

        };


        testRunner.enqueue(records2);
        testRunner.clearQueues();
        testRunner.run();

        ElasticsearchUtils.refreshIndex(client, indexName);

        Assert.assertEquals(1, ElasticsearchUtils.countIndex(client, indexName));
        searchResponse = client.prepareSearch(indexName)
                .setTypes(recordType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();
        Assert.assertEquals(1, searchResponse.getHits().getTotalHits());

        esResponse = searchResponse.getHits().getAt(0).getSourceAsString();
        assertNotNull(esResponse);
        assertTrue(esResponse.contains("POST"));
    }
}
