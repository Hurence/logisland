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
package com.hurence.logisland.processor;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.util.BaseSyslogTest;
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
import com.hurence.logisland.service.cache.LRUKeyValueCacheService;
import com.hurence.logisland.service.cache.model.Cache;
import com.hurence.logisland.service.cache.model.LRUCache;

import java.io.IOException;
import java.util.Map;

import static com.hurence.logisland.processor.SourceOfTraffic.ES_INDEX_FIELD;

public class SourceOfTrafficTest extends BaseSyslogTest {

    private static final Logger logger = LoggerFactory.getLogger(SourceOfTrafficTest.class);

    private Record getRecord1() {
        Record record1 = new StandardRecord();
        record1.setField("referer",   FieldType.STRING, "xyz_website");
        record1.setField("utm_source",         FieldType.STRING, "mysource");
        record1.setField("utm_campaign",       FieldType.STRING, "mycampaign");
        record1.setField("utm_medium",         FieldType.STRING, "email");
        record1.setField("utm_content",        FieldType.STRING, "mycontent");
        record1.setField("utm_keyword",        FieldType.STRING, "mykeyword");
        return record1;
    }

    @Test
    public void testUtmSource() throws InitializationException {
        Record record1 = getRecord1();

        TestRunner testRunner = getTestRunner();

        testRunner.assertValid();
        testRunner.setProperty("cache.size", "5");
        testRunner.setProperty("debug", "true");
        testRunner.setProperty("source.out.field", "source_of_traffic");
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        Field field = out.getField("source_of_traffic");
        out.assertRecordSizeEquals(7);
        out.assertFieldTypeEquals("source_of_traffic", FieldType.MAP);
    }

    private TestRunner getTestRunner() throws InitializationException {

        final TestRunner runner = TestRunners.newTestRunner(SourceOfTraffic.class);

        // create the controller service and link it to the test processor
        final MockElasticsearchClientService elasticsearchClient = new MockElasticsearchClientService();
        runner.addControllerService("elasticsearchClient", elasticsearchClient);
        runner.enableControllerService(elasticsearchClient);
        runner.setProperty(SourceOfTraffic.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");

        final MockCacheService<String, String> cacheService = new MockCacheService(20);
        runner.addControllerService("cacheService", cacheService);
        runner.enableControllerService(cacheService);
        runner.setProperty(SourceOfTraffic.CONFIG_CACHE_SERVICE, "cacheService");

        runner.setProperty(ES_INDEX_FIELD.getName(), "index1");

        return runner;
    }

    private class MockCacheService<K,V> extends LRUKeyValueCacheService<K,V> {

        private int cacheSize;

        public MockCacheService(final int cacheSize) {
            this.cacheSize = cacheSize;
        }

        @Override
        protected Cache<K, V> createCache(ControllerServiceInitializationContext context) throws IOException, InterruptedException {
            return new LRUCache<K,V>(cacheSize);
        }
    }

}
