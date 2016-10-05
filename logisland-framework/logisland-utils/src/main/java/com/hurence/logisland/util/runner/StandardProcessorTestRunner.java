/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.util.runner;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.ValidationResult;
import com.hurence.logisland.processor.*;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class StandardProcessorTestRunner implements TestRunner {

    private final Processor processor;
   // private final StandardProcessorInstance instance;
    private final MockProcessContext context;
    private final RecordQueue inputRecordsQueue;
    private final RecordQueue outputRecordsQueue;
    private static Logger logger = LoggerFactory.getLogger(StandardProcessorTestRunner.class);
    private static final AtomicLong currentId = new AtomicLong(0);

    StandardProcessorTestRunner(final Processor processor) {
        this.processor = processor;
        this.inputRecordsQueue = new RecordQueue();
        this.outputRecordsQueue = new RecordQueue();
       // this.instance = new StandardProcessorInstance(processor, Long.toString(currentId.incrementAndGet()));
        this.context = new MockProcessContext(processor);
        this.processor.init(context);
    }


    @Override
    public Processor getProcessor() {
        return processor;
    }

    @Override
    public ProcessContext getProcessContext() {
        return context;
    }


    @Override
    public void run() {
        while(!inputRecordsQueue.isEmpty()){
            processor.process(context, inputRecordsQueue.poll());
        }
    }


    @Override
    public void assertValid() {
        assertTrue("Processor is invalid", context.isValid());
    }

    @Override
    public void assertNotValid() {
        assertFalse("Processor appears to be valid but expected it to be invalid", context.isValid());
    }


    @Override
    public void enqueue(final Record... records) {
        for (final Record record : records) {
            inputRecordsQueue.offer(record);
        }
    }



    @Override
    public void enqueue( final String key, String value) {
        final Record record = RecordUtils.getKeyValueRecord(key,value);
        enqueue(record);
    }



    @Override
    public boolean removeProperty(PropertyDescriptor descriptor) {
        return context.removeProperty(descriptor);
    }

    @Override
    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        return context.setProperty(propertyName, propertyValue);
    }

    @Override
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final String value) {
        return context.setProperty(descriptor, value);
    }

    @Override
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final AllowableValue value) {
        return context.setProperty(descriptor, value.getValue());
    }

    @Override
    public void setAnnotationData(String annotationData) {

    }

    @Override
    public void assertAllRecordsProcessed(int count) {

    }

    @Override
    public void assertAllRecordsContainAttribute(String attributeName) {

    }

    @Override
    public void assertAllRecords(RecordValidator validator) {

    }


    @Override
    public String getVariableValue(final String name) {
        Objects.requireNonNull(name);

        return null;
        // return variableRegistry.getVariableValue(name);
    }

    @Override
    public void setVariable(final String name, final String value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);

       /* final VariableDescriptor descriptor = new VariableDescriptor.Builder(name).build();
        variableRegistry.setVariable(descriptor, value);*/
    }

    @Override
    public String removeVariable(final String name) {
        Objects.requireNonNull(name);

        return null;
        // return variableRegistry.removeVariable(new VariableDescriptor.Builder(name).build());
    }
}

