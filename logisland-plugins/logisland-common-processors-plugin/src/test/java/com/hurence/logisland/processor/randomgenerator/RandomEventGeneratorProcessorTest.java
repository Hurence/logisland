package com.hurence.logisland.processor.randomgenerator;

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


public class RandomEventGeneratorProcessorTest {

    private static Logger logger = LoggerFactory.getLogger(RandomEventGeneratorProcessorTest.class);


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
        conf.put("avro.input.schema", avroSchema);
        conf.put("min.events.count", "5");
        conf.put("max.events.count", "20");

        ComponentConfiguration componentConfiguration = new ComponentConfiguration();

        componentConfiguration.setComponent("com.hurence.logisland.processor.randomgenerator.RandomEventGeneratorProcessor");
        componentConfiguration.setType("processor");
        componentConfiguration.setConfiguration(conf);

        StandardProcessorInstance instance = ComponentsFactory.getProcessorInstance(componentConfiguration);
        ProcessContext context = new StandardProcessContext(instance);
        assert instance != null;
        instance.getProcessor().init(context);
        Collection<Event> events = instance.getProcessor().process(context, Collections.emptyList());

        Assert.assertTrue(events.size() <= 20);
        Assert.assertTrue(events.size() >= 5);
    }
}
