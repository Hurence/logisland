package com.hurence.logisland.processor;

import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.StandardComponentContext;
import com.hurence.logisland.config.ComponentFactory;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.utils.string.Multiline;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class RandomRecordGeneratorTest {

    private static Logger logger = LoggerFactory.getLogger(RandomRecordGeneratorTest.class);


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
        conf.put("avro.output.schema", avroSchema);
        conf.put("min.events.count", "5");
        conf.put("max.events.count", "20");

        ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();

        componentConfiguration.setComponent("com.hurence.logisland.processor.RandomRecordGenerator");
        componentConfiguration.setType("processor");
        componentConfiguration.setConfiguration(conf);

        Optional<StandardProcessorInstance> instance = ComponentFactory.getProcessorInstance(componentConfiguration);
        assert instance.isPresent();
        ComponentContext context = new StandardComponentContext(instance.get());

        Assert.assertTrue(instance.get().isValid());

        Collection<Record> records = instance.get().getProcessor().process(context, Collections.emptyList());

        Assert.assertTrue(records.size() <= 20);
        Assert.assertTrue(records.size() >= 5);
    }
}
