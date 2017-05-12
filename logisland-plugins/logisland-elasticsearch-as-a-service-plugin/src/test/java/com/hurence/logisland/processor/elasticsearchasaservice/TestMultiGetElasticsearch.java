package com.hurence.logisland.processor.elasticsearchasaservice;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
        final String index2 = "index2";
        final String type1 = "type1";
        final String type2 = "type2";
        final String type3 = "type3";
        final String docId1 = "id1";
        final String docId2 = "id2";
        final String docId3 = "id3";

        //////////////////
        final TestRunner runner = TestRunners.newTestRunner(MultiGetElasticsearch.class);
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
                .setField(ES_IDS_FIELD, FieldType.ARRAY, new ArrayList<>(Arrays.asList(docId1, docId2, docId3)))
                .setField(ES_INCLUDES_FIELD, FieldType.ARRAY, new ArrayList<>(Arrays.asList("field_b*", "field*1")))
                .setField(ES_EXCLUDES_FIELD, FieldType.ARRAY, new ArrayList<>(Arrays.asList("field_*2")));

        final Record inputRecord2 = new StandardRecord("es_multiget")
                .setStringField(ES_INDEX_FIELD, index1)
                .setStringField(ES_TYPE_FIELD, type1)
                .setField(ES_IDS_FIELD, FieldType.ARRAY, new ArrayList<>(Arrays.asList(docId1, docId2)))
                .setField(ES_INCLUDES_FIELD, FieldType.ARRAY, new ArrayList<>(Arrays.asList("field_b*", "field*1")))
                .setField(ES_EXCLUDES_FIELD, FieldType.ARRAY, new ArrayList<>(Arrays.asList("field_*2")));

        runner.enqueue(inputRecord1);
        runner.enqueue(inputRecord2);
        runner.clearQueues();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(5);
        runner.assertOutputErrorCount(0);


    }

}