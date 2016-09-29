package com.hurence.logisland.processor;

import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.component.StandardComponentContext;
import com.hurence.logisland.config.ComponentFactory;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.record.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;


public class MockProcessorTest {


    @Test
    public void validateProcess() throws Exception {

        String message = "logisland rocks !";
        Map<String, String> conf = new HashMap<>();
        conf.put(MockProcessor.FAKE_MESSAGE.getName(), message );

        ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();
        componentConfiguration.setComponent(MockProcessor.class.getName());
        componentConfiguration.setType(ComponentType.PROCESSOR.toString());
        componentConfiguration.setConfiguration(conf);

        Optional<StandardProcessorInstance> instance = ComponentFactory.getProcessorInstance(componentConfiguration);
        assertTrue(instance.isPresent());
        ComponentContext context = new StandardComponentContext(instance.get());
        Processor processor = instance.get().getProcessor();

        Record record = new Record("mock_record");
        record.setId("record1");
        record.setStringField("name", "tom");
        List<Record> records = new ArrayList<>(processor.process(context, Collections.singleton(record)));

        assertEquals(1, records.size());
        assertTrue(records.get(0).hasField("message"));
        assertEquals(message, records.get(0).getField("message").asString());

    }



}