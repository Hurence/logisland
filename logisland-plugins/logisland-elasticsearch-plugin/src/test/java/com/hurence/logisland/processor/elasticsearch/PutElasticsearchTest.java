package com.hurence.logisland.processor.elasticsearch;


import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.hurence.logisland.config.ComponentFactory;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.processor.StandardProcessContext;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.StandardProcessorInstance;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.utils.elasticsearch.ElasticsearchRecordConverter;
import com.hurence.logisland.utils.string.Multiline;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class PutElasticsearchTest extends ESIntegTestCase {


    private static Logger logger = LoggerFactory.getLogger(PutElasticsearchTest.class);




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
        String node = clusterService().localNode().address().toString();
        clusterService().localNode();
        conf.put("hosts", "localhost:9400" );
        conf.put("index", indexName);
        conf.put("type", "logisland");
        conf.put("cluster", cluster().getClusterName());

;


        ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();

        componentConfiguration.setComponent(PutElasticsearch.class.getName());
        componentConfiguration.setType("processor");
        componentConfiguration.setConfiguration(conf);

        Optional<StandardProcessorInstance> instance = ComponentFactory.getProcessorInstance(componentConfiguration);
        Assert.assertTrue(instance.isPresent());
        ProcessContext context = new StandardProcessContext(instance.get());


        Record record = new Record(recordType);
        record.setId("firewall_record1");
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

 //       instance.get().getProcessor().process(context, Collections.singletonList(record));




        // @TODO embed a local instance here

       index(indexName, recordType, "1", ElasticsearchRecordConverter.convert(record));
      /*   index("test", recordType, "2", jsonBuilder().startObject().field("text",
                "value2").endObject());*/
        refresh();
        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setTypes(recordType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("ip_source", "123.34.45.123"))                 // Query
                //   .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();


        assertHitCount(searchResponse, 1);
        assertFirstHit(searchResponse, hasId("1"));
    }
}
