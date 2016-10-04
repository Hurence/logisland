package com.hurence.logisland.processor.elasticsearch;


import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.hurence.logisland.config.ComponentFactory;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.StandardProcessContext;
import com.hurence.logisland.processor.StandardProcessorInstance;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, maxNumDataNodes = 2)
@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
public class PutElasticsearchTest extends ESIntegTestCase {


    private static Logger logger = LoggerFactory.getLogger(PutElasticsearchTest.class);

/*
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("node.mode", "network")
                .build();
    }
*/
    @Before
    private void setup() throws IOException {
        Client client = client();


        createIndex("test");
        ensureGreen("test");


    }


    @Test
    public void testLoadConfig() throws Exception {


        final String indexName = "test";
        final String recordType = "cisco_record";
        Map<String, String> conf = new HashMap<>();

        conf.put("hosts", "local[1]:9300");
        conf.put("default.type", recordType);
        conf.put("cluster.name", cluster().getClusterName());
        conf.put("default.index", indexName);


        ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();

        componentConfiguration.setComponent(PutElasticsearch.class.getName());
        componentConfiguration.setType("processor");
        componentConfiguration.setConfiguration(conf);

        Optional<StandardProcessorInstance> instance = ComponentFactory.getProcessorInstance(componentConfiguration);
        Assert.assertTrue(instance.isPresent());
        ProcessContext context = new StandardProcessContext(instance.get());


        Record record = new Record(recordType);
        record.setId("firewall_record1");
        record.setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L);
        record.setField("method", FieldType.STRING, "GET");
        record.setField("ip_source", FieldType.STRING, "123.34.45.123");
        record.setField("ip_target", FieldType.STRING, "255.255.255.255");
        record.setField("url_scheme", FieldType.STRING, "http");
        record.setField("url_host", FieldType.STRING, "origin-www.20minutes.fr");
        record.setField("url_port", FieldType.STRING, "80");
        record.setField("url_path", FieldType.STRING, "/r15lgc-100KB.js");
        record.setField("request_size", FieldType.INT, 1399);
        record.setField("response_size", FieldType.INT, 452);
        record.setField("is_outside_office_hours", FieldType.BOOLEAN, false);
        record.setField("is_host_blacklisted", FieldType.BOOLEAN, false);
        record.setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")));

        Record record2 = new Record(record);
        record2.setField("ip_source", FieldType.STRING, "123.34.45.12");
  //      record2.setField("response_size", FieldType.STRING, "-");

        PutElasticsearch processor = (PutElasticsearch) instance.get().getProcessor();
        processor.setClient(internalCluster().masterClient());
        processor.process(context, new ArrayList<>(Arrays.asList(record, record2)));


        flushAndRefresh();
        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setTypes(recordType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("ip_source", "123.34.45.123"))
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();


        assertHitCount(searchResponse, 1);
        assertEquals("{\"@timestamp\":\"2016-10-03T22:14:48+02:00\",\"ip_source\":\"123.34.45.123\",\"ip_target\":\"255.255.255.255\",\"is_host_blacklisted\":false,\"is_outside_office_hours\":false,\"method\":\"GET\",\"record_id\":\"firewall_record1\",\"record_time\":1475525688668,\"record_type\":\"cisco_record\",\"request_size\":1399,\"response_size\":452,\"tags\":[\"spam\",\"filter\",\"mail\"],\"url_host\":\"origin-www.20minutes.fr\",\"url_path\":\"/r15lgc-100KB.js\",\"url_port\":\"80\",\"url_scheme\":\"http\"}",searchResponse.getHits().getAt(0).getSourceAsString());
    }
}
