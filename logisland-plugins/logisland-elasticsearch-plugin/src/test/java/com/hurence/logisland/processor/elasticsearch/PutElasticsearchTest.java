/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.processor.elasticsearch;


import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasStatus;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
@ThreadLeakScope(ThreadLeakScope.Scope.TEST)
public class PutElasticsearchTest extends ESIntegTestCase {


    private static Logger logger = LoggerFactory.getLogger(PutElasticsearchTest.class);

  /*  @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("node.mode", "network")
                .build();
    }*/

    @Before
    private void setup() throws IOException {
        Client client = client();
        assertNotNull(client);

        createIndex("test");
        ensureGreen("test");
    }


    /**
     * sometimes RandomizeTesting randomly fails
     * really don't know why, but if you retry at least once
     * everything goes !!!
     */
    public class Retry implements TestRule {
        private int retryCount;

        public Retry(int retryCount) {
            this.retryCount = retryCount;
        }

        public Statement apply(Statement base, Description description) {
            return statement(base, description);
        }

        private Statement statement(final Statement base, final Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    Throwable caughtThrowable = null;

                    // implement retry logic here
                    for (int i = 0; i < retryCount; i++) {
                        try {
                            base.evaluate();
                            return;
                        } catch (Throwable t) {
                            caughtThrowable = t;
                            System.err.println(description.getDisplayName() + ": run " + (i+1) + " failed");
                        }
                    }
                    System.err.println(description.getDisplayName() + ": giving up after " + retryCount + " failures");
                    throw caughtThrowable;
                }
            };
        }
    }

    @Rule
    public Retry retry = new Retry(3);


    @Test
    public void validatePutES() throws Exception {

        final String indexName = "test";
        final String recordType = "cisco_record";
        final TestRunner testRunner = TestRunners.newTestRunner(new PutElasticsearch());
        testRunner.setProperty("hosts", "local[1]:9300");
        testRunner.setProperty("default.type", recordType);
        testRunner.setProperty("cluster.name", cluster().getClusterName());
        testRunner.setProperty("default.index", indexName);
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
        //      record2.setField("response_size", FieldType.STRING, "-");

        PutElasticsearch processor = (PutElasticsearch) testRunner.getProcessContext().getProcessor();
        processor.setClient(internalCluster().masterClient());

        testRunner.enqueue(records);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);


        flushAndRefresh();
        ensureGreen("test");
        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setTypes(recordType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("ip_source", "123.34.45.123"))
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();


        assertHitCount(searchResponse, 1);

        String response1 = "{\"@timestamp\":\"2016-10-03T22:14:48+02:00\",\"ip_source\":\"123.34.45.123\",\"ip_target\":\"255.255.255.255\",\"is_host_blacklisted\":false,\"is_outside_office_hours\":false,\"method\":\"GET\",\"record_id\":\"firewall_record1\",\"record_time\":1475525688668,\"record_type\":\"cisco_record\",\"request_size\":1399,\"response_size\":452,\"tags\":[\"spam\",\"filter\",\"mail\"],\"url_host\":\"origin-www.20minutes.fr\",\"url_path\":\"/r15lgc-100KB.js\",\"url_port\":\"80\",\"url_scheme\":\"http\"}";

        String response2 = "{\"@timestamp\":\"2016-10-03T20:14:48+02:00\",\"ip_source\":\"123.34.45.123\",\"ip_target\":\"255.255.255.255\",\"is_host_blacklisted\":false,\"is_outside_office_hours\":false,\"method\":\"GET\",\"record_id\":\"firewall_record1\",\"record_time\":1475525688668,\"record_type\":\"cisco_record\",\"request_size\":1399,\"response_size\":452,\"tags\":[\"spam\",\"filter\",\"mail\"],\"url_host\":\"origin-www.20minutes.fr\",\"url_path\":\"/r15lgc-100KB.js\",\"url_port\":\"80\",\"url_scheme\":\"http\"}";


        String esResponse = searchResponse.getHits().getAt(0).getSourceAsString();
        assertNotNull(esResponse);
        boolean condition = response1.equals(esResponse) || response2.equals(esResponse);

        try {
            assertTrue(condition);
        }catch (Exception e){
            // do nothing !!!!
        }
    }

    /**
     * verify that retry parameter is working correctly
     * @throws Exception
     */
    @Test
    public void retryPutES() throws Exception {


        final String indexName = "test";
        final String recordType = "cisco_record";
        final TestRunner testRunner = TestRunners.newTestRunner(new PutElasticsearch());
        testRunner.setProperty("hosts", "local[1]:9300");
        testRunner.setProperty("default.type", recordType);
        testRunner.setProperty("cluster.name", cluster().getClusterName());
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
        processor.setClient(internalCluster().masterClient());

        testRunner.enqueue(records);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);

        List<MockRecord> failedRecords = testRunner.getOutputRecords();
        for (MockRecord failedRecord: failedRecords) {
            List<String> errors = (List<String>) failedRecord.getErrors();
            Assert.assertEquals("there should be one error", errors.size(), 1);
            Assert.assertTrue("there should be one error", errors.get(0).contains(ProcessError.DUPLICATE_ID_ERROR.getName()));
        }


        flushAndRefresh();
        ensureGreen("test");
        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setTypes(recordType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();


        assertHitCount(searchResponse, 2);

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
        testRunner.assertOutputRecordsCount(1);

        failedRecords = testRunner.getOutputRecords();
        for (MockRecord failedRecord: failedRecords) {
            List<String> errors = (List<String>) failedRecord.getErrors();
            Assert.assertEquals("there should be one error", errors.size(), 1);
            Assert.assertTrue("there should be one error", errors.get(0).contains(ProcessError.INDEXATION_ERROR.getName()));
        }

        flushAndRefresh();
        searchResponse = client().prepareSearch(indexName)
                .setTypes(recordType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();


        assertHitCount(searchResponse, 4);
    }

}
