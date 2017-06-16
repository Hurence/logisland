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
package com.hurence.logisland.processor.elasticsearch;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class TestEnrichRecordsElasticsearch {

    private volatile Map<String/*id*/, String/*errors*/> errors = new HashMap<>();

    private static Logger logger = LoggerFactory.getLogger(TestEnrichRecordsElasticsearch.class);

    @Test
    public void testEnrichThreeRecords() throws IOException, InitializationException {

        final String RECORD_KEY_FIELD = "codeProduct";
        final String ES_TYPE = "es_type";
        final String ES_INDEX = "index1";
        final String ES_INCLUDES = "*";
        final String ES_EXCLUDES = "N/A";

        final String index1 = "index1";
        final String docId1 = "id1";
        final String docId2 = "id2";
        final String docId3 = "id3";

        //////////////////
        final TestRunner runner = TestRunners.newTestRunner(EnrichRecordsElasticsearch.class);

        runner.setProperty(EnrichRecordsElasticsearch.ES_TYPE_FIELD, ES_TYPE);
        runner.setProperty(EnrichRecordsElasticsearch.RECORD_KEY_FIELD, RECORD_KEY_FIELD);
        runner.setProperty(EnrichRecordsElasticsearch.ES_INDEX_FIELD, ES_INDEX);
        runner.setProperty(EnrichRecordsElasticsearch.ES_INCLUDES_FIELD, ES_INCLUDES);
        runner.setProperty(EnrichRecordsElasticsearch.ES_EXCLUDES_FIELD, ES_EXCLUDES);
        runner.setProperty(EnrichRecordsElasticsearch.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");

        runner.assertValid();

        ///////////////////
        final MockElasticsearchClientService elasticsearchClient = new MockElasticsearchClientService();
        runner.addControllerService("elasticsearchClient", elasticsearchClient);
        runner.enableControllerService(elasticsearchClient);

        ///////////////////
        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setStringField(RECORD_KEY_FIELD, docId1)
                .setStringField("category", "123456")
                .setStringField("price", "89");

        final Record inputRecord2 = new StandardRecord("es_multiget")
                .setStringField(RECORD_KEY_FIELD, docId2)
                .setStringField("category", "654321")
                .setStringField("price", "56");

        final Record inputRecord3 = new StandardRecord("es_multiget")
                .setStringField(RECORD_KEY_FIELD, docId3)
                .setStringField("category", "654321")
                .setStringField("price", "56");

        runner.enqueue(inputRecord1);
        runner.enqueue(inputRecord2);
        runner.enqueue(inputRecord3);
        runner.clearQueues();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(3);
        runner.assertOutputErrorCount(0);
    }

    @Test
    public void testEnrichOneRecordWithoutRecordKey() throws IOException, InitializationException {

        final String RECORD_KEY_FIELD = "codeProduct";
        final String ES_INDEX = "es_index";
        final String ES_TYPE = "es_type";
        final String ES_INCLUDES = "es_includes";
        final String ES_EXCLUDES = "es_excludes";

        //////////////////
        final TestRunner runner = TestRunners.newTestRunner(EnrichRecordsElasticsearch.class);
        runner.setProperty(EnrichRecordsElasticsearch.RECORD_KEY_FIELD, RECORD_KEY_FIELD);
        runner.setProperty(EnrichRecordsElasticsearch.ES_INDEX_FIELD, ES_INDEX);
        runner.setProperty(EnrichRecordsElasticsearch.ES_TYPE_FIELD, ES_TYPE);
        runner.setProperty(EnrichRecordsElasticsearch.ES_INCLUDES_FIELD, ES_INCLUDES);
        runner.setProperty(EnrichRecordsElasticsearch.ES_EXCLUDES_FIELD, ES_EXCLUDES);
        runner.setProperty(EnrichRecordsElasticsearch.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");

        runner.assertValid();

        ///////////////////
        final MockElasticsearchClientService elasticsearchClient = new MockElasticsearchClientService();
        runner.addControllerService("elasticsearchClient", elasticsearchClient);
        runner.enableControllerService(elasticsearchClient);

        ///////////////////
        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setStringField("category", "654321")
                .setStringField("price", "56");

        runner.enqueue(inputRecord1);
        runner.clearQueues();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(1);
        runner.assertOutputErrorCount(0);


    }

}
