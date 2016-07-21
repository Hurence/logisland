package com.hurence.logisland.processor.elasticsearch;

import com.hurence.logisland.components.ComponentsFactory;
import com.hurence.logisland.config.ComponentConfiguration;
import com.hurence.logisland.event.Event;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.StandardProcessContext;
import com.hurence.logisland.processor.StandardProcessorInstance;
import com.hurence.logisland.utils.string.Multiline;
import org.junit.Assert;
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
        Event event = new Event("cisco");
        event.put("timestamp", "Long", new Date().getTime());
        event.put("method", "String", "GET");
        event.put("ipSource", "String", "123.34.45.123");
        event.put("ipTarget", "String", "178.23.45.234");
        event.put("urlScheme", "String", "http");
        event.put("urlHost", "String", "hurence.com");
        event.put("urlPort", "String", "80");
        event.put("urlPath", "String", "idea/help/create-test.html");
        event.put("requestSize", "Int", 4578);
        event.put("responseSize", "Int", 452);
        event.put("isOutsideOfficeHours", "Boolean", true);
        event.put("isHostBlacklisted", "Boolean", false);
        event.put("tags", "String", "spam,filter,mail");

        instance.getProcessor().init(context);
        instance.getProcessor().process(context, Collections.singletonList(event));

        // @TODO embed a local instance here
    }
}
