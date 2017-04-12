/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.processor;

import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockProcessor;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class MockProcessorTest {


    @Test
    public void validateProcess() throws Exception {

        String message = "logisland rocks !";
        Map<String, String> conf = new HashMap<>();
        conf.put(MockProcessor.FAKE_MESSAGE.getName(), message);

        ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();
        componentConfiguration.setComponent(MockProcessor.class.getName());
        componentConfiguration.setType(ComponentType.PROCESSOR.toString());
        componentConfiguration.setConfiguration(conf);

        Optional<ProcessContext> context = ComponentFactory.getProcessContext(componentConfiguration);
        assertTrue(context.isPresent());
        Processor processor = context.get().getProcessor();

        Record record = new StandardRecord("mock_record");
        record.setId("record1");
        record.setStringField("name", "tom");
        List<Record> records = new ArrayList<>(processor.process(context.get(), Collections.singleton(record)));

        assertEquals(1, records.size());
        assertTrue(records.get(0).hasField("message"));
        assertEquals(message, records.get(0).getField("message").asString());

    }


}