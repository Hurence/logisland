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


public class TestMultiGetElasticsearch {

    private volatile Map<String/*id*/, String/*errors*/> errors = new HashMap<>();

    private static Logger logger = LoggerFactory.getLogger(TestMultiGetElasticsearch.class);

    @Test
    public void testMultiGetTwoRecords() throws IOException, InitializationException {

        final String ES_INDEX_FIELD = "es_index";
        final String ES_TYPE_FIELD = "es_type";
        final String ES_IDS_FIELD = "es_document_ids";
        final String ES_INCLUDES_FIELD = "es_includes";
        final String ES_EXCLUDES_FIELD = "es_excludes";

        final String index1 = "index1";
        final String type1 = "type1";
        final String docId1 = "id1";
        final String docId2 = "id2";
        final String docId3 = "id3";

        //////////////////
        final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.elasticsearch.MultiGetElasticsearch");
        runner.setProperty(MultiGetElasticsearch.ES_INDEX_FIELD, ES_INDEX_FIELD);
        runner.setProperty(MultiGetElasticsearch.ES_TYPE_FIELD, ES_TYPE_FIELD);
        runner.setProperty(MultiGetElasticsearch.ES_IDS_FIELD, ES_IDS_FIELD);
        runner.setProperty(MultiGetElasticsearch.ES_INCLUDES_FIELD, ES_INCLUDES_FIELD);
        runner.setProperty(MultiGetElasticsearch.ES_EXCLUDES_FIELD, ES_EXCLUDES_FIELD);
        runner.setProperty(MultiGetElasticsearch.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");

        runner.assertValid();

        ///////////////////
        final MockElasticsearchClientService elasticsearchClient = new MockElasticsearchClientService();
        runner.addControllerService("elasticsearchClient", elasticsearchClient);
        runner.enableControllerService(elasticsearchClient);

        ///////////////////
        final Record inputRecord1 = new StandardRecord("es_multiget")
                .setStringField(ES_INDEX_FIELD, index1)
                .setStringField(ES_TYPE_FIELD, type1)
                .setStringField(ES_IDS_FIELD, docId1 + "," + docId2 + "," + docId3)
                .setStringField(ES_INCLUDES_FIELD, "field_b*, field*1")
                .setStringField(ES_EXCLUDES_FIELD, "field_*2");

        final Record inputRecord2 = new StandardRecord("es_multiget")
                .setStringField(ES_INDEX_FIELD, index1)
                .setStringField(ES_TYPE_FIELD, type1)
                .setStringField(ES_IDS_FIELD, docId1 + "," + docId2)
                .setStringField(ES_INCLUDES_FIELD, "field_b*, field*1")
                .setStringField(ES_EXCLUDES_FIELD, "field_*2");

        runner.enqueue(inputRecord1);
        runner.enqueue(inputRecord2);
        runner.clearQueues();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(5);
        runner.assertOutputErrorCount(0);
    }

    @Test
    public void testMultiGetCorruptedRecords() throws IOException, InitializationException {

        final String ES_INDEX_FIELD = "es_index";
        final String ES_TYPE_FIELD = "es_type";
        final String ES_IDS_FIELD = "es_document_ids";
        final String ES_INCLUDES_FIELD = "es_includes";
        final String ES_EXCLUDES_FIELD = "es_excludes";

        final String index1 = "index1";
        final String index2 = "index2";
        final String type1 = "type1";
        final String type2 = "type2";
        final String type3 = "type3";
        final String docId1 = "id1";
        final String docId2 = "id2";
        final String docId3 = "id3";

        //////////////////
        final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.elasticsearch.MultiGetElasticsearch");
        runner.setProperty(MultiGetElasticsearch.ES_INDEX_FIELD, ES_INDEX_FIELD);
        runner.setProperty(MultiGetElasticsearch.ES_TYPE_FIELD, ES_TYPE_FIELD);
        runner.setProperty(MultiGetElasticsearch.ES_IDS_FIELD, ES_IDS_FIELD);
        runner.setProperty(MultiGetElasticsearch.ES_INCLUDES_FIELD, ES_INCLUDES_FIELD);
        runner.setProperty(MultiGetElasticsearch.ES_EXCLUDES_FIELD, ES_EXCLUDES_FIELD);
        runner.setProperty(MultiGetElasticsearch.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");

        runner.assertValid();

        ///////////////////
        final MockElasticsearchClientService elasticsearchClient = new MockElasticsearchClientService();
        runner.addControllerService("elasticsearchClient", elasticsearchClient);
        runner.enableControllerService(elasticsearchClient);

        ///////////////////

        // index field missing --> 1 output error record
        final Record inputRecord1 = new StandardRecord("es_multiget")
                // index field is intentionally missing : .setStringField(ES_INDEX_FIELD, index1)
                .setStringField(ES_TYPE_FIELD, type1)
                .setStringField(ES_IDS_FIELD, docId1 + "," + docId2 + "," + docId3)
                .setStringField(ES_INCLUDES_FIELD, "field_b*, field*1")
                .setStringField(ES_EXCLUDES_FIELD, "field_*2");

        // index field empty --> 1 output error record
        final Record inputRecord2 = new StandardRecord("es_multiget")
                .setStringField(ES_INDEX_FIELD, "")
                .setStringField(ES_TYPE_FIELD, type1)
                .setStringField(ES_IDS_FIELD, docId1 + "," + docId2 + "," + docId3)
                .setStringField(ES_INCLUDES_FIELD, "field_b*, field*1")
                .setStringField(ES_EXCLUDES_FIELD, "field_*2");

        // index field null --> 1 output error record
        final Record inputRecord3 = new StandardRecord("es_multiget")
                .setStringField(ES_INDEX_FIELD, null)
                .setStringField(ES_TYPE_FIELD, type1)
                .setStringField(ES_IDS_FIELD, docId1 + "," + docId2 + "," + docId3)
                .setStringField(ES_INCLUDES_FIELD, "field_b*, field*1")
                .setStringField(ES_EXCLUDES_FIELD, "field_*2");

        // type field missing --> no problem --> 3 document ids <==> 3 output records
        final Record inputRecord4 = new StandardRecord("es_multiget")
                .setStringField(ES_INDEX_FIELD, index1)
                // type field is intentionally missing : .setStringField(ES_TYPE_FIELD, type1)
                .setStringField(ES_IDS_FIELD, docId1 + "," + docId2 + "," + docId3)
                .setStringField(ES_INCLUDES_FIELD, "field_b*, field*1")
                .setStringField(ES_EXCLUDES_FIELD, "field_*2");

        // document ids field missing --> 1 output error record
        final Record inputRecord5 = new StandardRecord("es_multiget")
                .setStringField(ES_INDEX_FIELD, index1)
                .setStringField(ES_TYPE_FIELD, type1)
                // document ids field is intentionally missing : .setField(ES_IDS_FIELD, docId1 + "," + docId2)
                .setStringField(ES_INCLUDES_FIELD, "field_b*,field*1")
                .setStringField(ES_EXCLUDES_FIELD, "field_*2");

        // includes and excludes fields missing --> no problem --> 2 document ids <==> 2 output records
        final Record inputRecord6 = new StandardRecord("es_multiget")
                .setStringField(ES_INDEX_FIELD, index1)
                .setStringField(ES_TYPE_FIELD, type2)
                .setStringField(ES_IDS_FIELD, docId1 + "," + docId2);

        // includes and excludes fields null and type field missing --> no problem --> 3 document ids <==> 3 output records
        final Record inputRecord7 = new StandardRecord("es_multiget")
                .setStringField(ES_INDEX_FIELD, index1)
                //.setStringField(ES_TYPE_FIELD, type1)
                .setStringField(ES_IDS_FIELD, docId1 + "," + docId2 + "," + docId3)
                .setStringField(ES_INCLUDES_FIELD, null)
                .setStringField(ES_EXCLUDES_FIELD, null);

        runner.enqueue(inputRecord1);
        runner.enqueue(inputRecord2);
        runner.enqueue(inputRecord3);
        runner.enqueue(inputRecord4);
        runner.enqueue(inputRecord5);
        runner.enqueue(inputRecord6);
        runner.enqueue(inputRecord7);
        runner.clearQueues();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(8);
        runner.assertOutputErrorCount(4);


    }

}