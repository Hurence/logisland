package com.hurence.logisland.processor.elasticsearch;

import com.hurence.logisland.components.ComponentsFactory;
import com.hurence.logisland.config.ComponentConfiguration;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.StandardProcessContext;
import com.hurence.logisland.processor.StandardProcessorInstance;
import com.hurence.logisland.utils.string.Multiline;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by tom on 21/07/16.
 */
public class PutElasticsearchTest {



    private static Logger logger = LoggerFactory.getLogger(PutElasticsearchTest.class);


    /**
     * {
     * "version": 1,
     * "type": "record",
     * "namespace": "com.hurence.logisland",
     * "name": "Event",
     * "fields": [
     * {
     * "name": "_type",
     * "type": "string"
     * },
     * {
     * "name": "_id",
     * "type": "string"
     * },
     * {
     * "name": "timestamp",
     * "type": "long"
     * },
     * {
     * "name": "method",
     * "type": "string"
     * },
     * {
     * "name": "ipSource",
     * "type": "string"
     * },
     * {
     * "name": "ipTarget",
     * "type": "string"
     * },
     * {
     * "name": "urlScheme",
     * "type": "string"
     * },
     * {
     * "name": "urlHost",
     * "type": "string"
     * },
     * {
     * "name": "urlPort",
     * "type": "string"
     * },
     * {
     * "name": "urlPath",
     * "type": "string"
     * },
     * {
     * "name": "requestSize",
     * "type": "int"
     * },
     * {
     * "name": "responseSize",
     * "type": "int"
     * },
     * {
     * "name": "isOutsideOfficeHours",
     * "type": "boolean"
     * },
     * {
     * "name": "isHostBlacklisted",
     * "type": "boolean"
     * },
     * {
     * "name": "tags",
     * "type": {
     * "type": "array",
     * "items": "string"
     * }
     * }
     * ]
     * }
     */
    @Multiline
    public static String avroSchema;


    @Test
    public void testLoadConfig() throws Exception {


        Map<String, String> conf = new HashMap<>();
        conf.put("hosts", "localhost:9300");
        conf.put("index", "test");
        conf.put("type", "logisland");

        ComponentConfiguration componentConfiguration = new ComponentConfiguration();

        componentConfiguration.setComponent("com.hurence.logisland.processor.elasticsearch.PutElasticsearch");
        componentConfiguration.setType("processor");
        componentConfiguration.setConfiguration(conf);

        StandardProcessorInstance instance = ComponentsFactory.getProcessorInstance(componentConfiguration);
        ProcessContext context = new StandardProcessContext(instance);
        assert instance != null;
        instance.getProcessor().init(context);
        //Collection<Event> events = instance.getProcessor().process(context, Collections.emptyList());

        // create an event
        Record record = new Record("cisco");
        record.setField("timestamp", "Long", new Date().getTime());
        record.setField("method", "String", "GET");
        record.setField("ipSource", "String", "123.34.45.123");
        record.setField("ipTarget", "String", "178.23.45.234");
        record.setField("urlScheme", "String", "http");
        record.setField("urlHost", "String", "hurence.com");
        record.setField("urlPort", "String", "80");
        record.setField("urlPath", "String", "idea/help/create-test.html");
        record.setField("requestSize", "Int", 4578);
        record.setField("responseSize", "Int", 452);
        record.setField("isOutsideOfficeHours", "Boolean", true);
        record.setField("isHostBlacklisted", "Boolean", false);
        record.setField("tags", "String", "spam,filter,mail");

        instance.getProcessor().init(context);
        instance.getProcessor().process(context, Collections.singletonList(record));

        // @TODO embed a local instance here
    }
}
