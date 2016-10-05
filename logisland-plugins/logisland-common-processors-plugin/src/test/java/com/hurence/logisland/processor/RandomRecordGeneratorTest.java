package com.hurence.logisland.processor;

import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.ComponentFactory;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.string.Multiline;
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
    public String avroSchema;


    @Test
    public void testLoadConfig() throws Exception {


        Map<String, String> conf = new HashMap<>();
        conf.put(RandomRecordGenerator.OUTPUT_SCHEMA.getName(), avroSchema);
        conf.put(RandomRecordGenerator.MIN_EVENTS_COUNT.getName(), "5");
        conf.put(RandomRecordGenerator.MAX_EVENTS_COUNT.getName(), "20");

        ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();

        componentConfiguration.setComponent(RandomRecordGenerator.class.getName());
        componentConfiguration.setType(ComponentType.PROCESSOR.toString());
        componentConfiguration.setConfiguration(conf);

        Optional<StandardProcessorInstance> instance = ComponentFactory.getProcessorInstance(componentConfiguration);
        assert instance.isPresent();
        ProcessContext context = new StandardProcessContext(instance.get());

        Assert.assertTrue(instance.get().isValid());

        Collection<Record> records = instance.get().getProcessor().process(context, Collections.emptyList());

        Assert.assertTrue(records.size() <= 20);
        Assert.assertTrue(records.size() >= 5);
    }
}
