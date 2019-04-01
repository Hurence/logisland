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
package com.hurence.logisland.processor.webAnalytics;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hurence.logisland.processor.webAnalytics.setSourceOfTraffic.ES_INDEX_FIELD;

public class setSourceOfTrafficTest {

    private static final Logger logger = LoggerFactory.getLogger(setSourceOfTrafficTest.class);

    private Record getRecord1() {
        Record record1 = new StandardRecord();
        record1.setField("referer", FieldType.STRING, "xyz_website");
        record1.setField("utm_source", FieldType.STRING, "mysource");
        record1.setField("utm_campaign", FieldType.STRING, "mycampaign");
        record1.setField("utm_medium", FieldType.STRING, "email");
        record1.setField("utm_content", FieldType.STRING, "mycontent");
        record1.setField("utm_keyword", FieldType.STRING, "mykeyword");
        return record1;
    }

    private Record getRecord2() {
        Record record1 = new StandardRecord();
        record1.setField("referer", FieldType.STRING, "xyz_website");
        record1.setField("utm_source", FieldType.STRING, "mysource");
        record1.setField("utm_campaign", FieldType.STRING, "mycampaign");
        return record1;
    }

    private Record getRecord3() {
        Record record1 = new StandardRecord();
        record1.setField("referer", FieldType.STRING, null);
        record1.setField("firstVisitedPage", FieldType.STRING, "https://www.xyz_website.com/home/index.html");
        return record1;
    }

    private Record getRecord4() {
        Record record1 = new StandardRecord();
        record1.setField("referer", FieldType.STRING, "https://www.xyz_website.com/fr/category/c-35");
        record1.setField("firstVisitedPage", FieldType.STRING, "https://www.xyz_website.com/fr/index.html");
        return record1;
    }

    private Record getRecord5() {
        Record record1 = new StandardRecord();
        record1.setField("referer", FieldType.STRING, "https://www.myrefering_site.com/fr/category/c-35");
        record1.setField("firstVisitedPage", FieldType.STRING, "https://www.xyz_website.com/fr/index.html");
        return record1;
    }

    @Test
    public void testUtmSource1() throws InitializationException {
        Record record1 = getRecord1();

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source.out.field", "source_of_traffic");
        testRunner.setProperty("source_of_traffic.hierarchical", "false");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(11);
    }

    @Test
    public void testUtmSource2() throws InitializationException {
        Record record1 = getRecord2();

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source.out.field", "source_of_traffic");
        testRunner.setProperty("source_of_traffic.hierarchical", "false");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        Field field = out.getField("source_of_traffic");
        out.assertRecordSizeEquals(6);
    }

    @Test
    public void testUtmSource_WithHierarchical() throws InitializationException {
        Record record1 = getRecord1();

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source_of_traffic.suffix", "source_of_traffic");
        testRunner.setProperty("source_of_traffic.hierarchical", "true");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        Field field = out.getField("source_of_traffic");
        out.assertRecordSizeEquals(7);
        out.assertFieldTypeEquals("source_of_traffic", FieldType.MAP);
    }


    @Test
    public void testDirectTraffic() throws InitializationException {
        Record record1 = getRecord3();

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source.out.field", "source_of_traffic");
        testRunner.setProperty("source_of_traffic.hierarchical", "false");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        Field field = out.getField("source_of_traffic");
        out.assertFieldEquals("source_of_traffic_source", "direct");
        out.assertFieldEquals("source_of_traffic_medium", "");
        out.assertFieldEquals("source_of_traffic_campaign", "direct");

    }

    @Test
    public void testRefererUnderWebsiteDomain() throws InitializationException {
        Record record1 = getRecord4();

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source.out.field", "source_of_traffic");
        testRunner.setProperty("source_of_traffic.hierarchical", "false");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("source_of_traffic_source", "direct");
        out.assertFieldEquals("source_of_traffic_medium", "");
        out.assertFieldEquals("source_of_traffic_campaign", "direct");
    }

    @Test
    public void testReferring() throws InitializationException {
        Record record1 = getRecord5();

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source.out.field", "source_of_traffic");
        testRunner.setProperty("source_of_traffic.hierarchical", "false");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("source_of_traffic_source", "myrefering_site");
        out.assertFieldEquals("source_of_traffic_medium", "referral");
    }

    @Test
    public void testAdwords() throws InitializationException {
        // Test the adword case with presence of gclid parameter.
        Record record1 = new StandardRecord();
        record1.setField("firstVisitedPage", FieldType.STRING, "https://www.xyz_website.com/fr/index.html?gclid=XXX");

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source.out.field", "source_of_traffic");
        testRunner.setProperty("source_of_traffic.hierarchical", "false");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("source_of_traffic_source", "google");
        out.assertFieldEquals("source_of_traffic_medium", "cpc");
        out.assertFieldEquals("source_of_traffic_campaign", "adwords");
        out.assertFieldEquals("source_of_traffic_content", "adwords");
        out.assertFieldEquals("source_of_traffic_keyword", "adwords");
    }

    @Test
    public void testDoubleClick() throws InitializationException {
        // Test the DoubleClick case with presence of gclsrc parameter.
        Record record1 = new StandardRecord();
        record1.setField("firstVisitedPage", FieldType.STRING, "https://www.xyz_website.com/fr/index.html?gclsrc=XXX");

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source.out.field", "source_of_traffic");
        testRunner.setProperty("source_of_traffic.hierarchical", "false");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("source_of_traffic_source", "google");
        out.assertFieldEquals("source_of_traffic_medium", "cpc");
        out.assertFieldEquals("source_of_traffic_campaign", "DoubleClick");
        out.assertFieldEquals("source_of_traffic_content", "DoubleClick");
        out.assertFieldEquals("source_of_traffic_keyword", "DoubleClick");
    }

    @Test
    public void testCampaign() throws InitializationException {
        // Test the adwords case with presence of campaign parameter.
        Record record1 = new StandardRecord();
        record1.setField("firstVisitedPage", FieldType.STRING, "https://www.orexad.com/fr/index.html?campaign=YYY&gclid=XXX");

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source.out.field", "source_of_traffic");
        testRunner.setProperty("source_of_traffic.hierarchical", "false");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("source_of_traffic_source", "google");
        out.assertFieldEquals("source_of_traffic_medium", "cpc");
        out.assertFieldEquals("source_of_traffic_campaign", "YYY");
        out.assertFieldEquals("source_of_traffic_content", "adwords");
        out.assertFieldEquals("source_of_traffic_keyword", "adwords");
    }

    @Test
    public void testAdwordsReferer() throws InitializationException {
        // Test the adwords case in referer.
        Record record1 = new StandardRecord();
        record1.setField("referer", FieldType.STRING, "https://www.orexad.com/fr/index.html?campaign=YYY&gclid=XXX");

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source.out.field", "source_of_traffic");
        testRunner.setProperty("source_of_traffic.hierarchical", "false");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("source_of_traffic_source", "google");
        out.assertFieldEquals("source_of_traffic_medium", "cpc");
        out.assertFieldEquals("source_of_traffic_campaign", "YYY");
        out.assertFieldEquals("source_of_traffic_content", "adwords");
        out.assertFieldEquals("source_of_traffic_keyword", "adwords");
    }

    private TestRunner getTestRunner() throws InitializationException {

        final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.webAnalytics.setSourceOfTraffic");

        // create the controller service and link it to the test processor
        final MockElasticsearchClientService elasticsearchClient = new MockElasticsearchClientService();
        runner.addControllerService("elasticsearchClient", elasticsearchClient);
        runner.enableControllerService(elasticsearchClient);
        runner.setProperty(setSourceOfTraffic.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");

        final MockCacheService<String, String> cacheService = new MockCacheService();
        runner.addControllerService("cacheService", cacheService);
        runner.enableControllerService(cacheService);
        runner.setProperty(setSourceOfTraffic.CONFIG_CACHE_SERVICE, "cacheService");

        runner.setProperty(ES_INDEX_FIELD.getName(), "index1");

        return runner;
    }


}
