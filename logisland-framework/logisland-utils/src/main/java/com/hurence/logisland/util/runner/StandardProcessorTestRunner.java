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
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.processor.MockProcessContext;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class StandardProcessorTestRunner implements TestRunner {

    private final Processor processor;
    private final ProcessContext context;
    private final List<Record> inputRecordsQueue;
    private final List<Record> outputRecordsList;
    private static Logger logger = LoggerFactory.getLogger(StandardProcessorTestRunner.class);
    private static final AtomicLong currentId = new AtomicLong(0);

    StandardProcessorTestRunner(final ProcessContext processContext) {
		this.processor = processContext.getProcessor();
		this.inputRecordsQueue = new ArrayList<>();
		this.outputRecordsList = new ArrayList<>();
		this.context = processContext;
	}
    
    StandardProcessorTestRunner(final Processor processor) {
        this.processor = processor;
        this.inputRecordsQueue = new ArrayList<>();
        this.outputRecordsList = new ArrayList<>();
        this.context = new MockProcessContext(processor);
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
        this.processor.init(context);
        Collection<Record> outputRecords = processor.process(context, inputRecordsQueue);
        outputRecordsList.addAll(outputRecords);
        inputRecordsQueue.clear();
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
            inputRecordsQueue.add(record);
        }
    }


    @Override
    public void enqueue(final String key, String value) {

        final Record record = RecordUtils.getKeyValueRecord(key, value);
        enqueue(record);

    }


    @Override
    public void enqueue(String keyValueSeparator, InputStream inputStream) {
        try {
            InputStreamReader isr = new InputStreamReader(inputStream, "UTF-8");
            BufferedReader bsr = new BufferedReader(isr);
            String line;
            while ((line = bsr.readLine()) != null) {

                if (keyValueSeparator == null || keyValueSeparator.isEmpty()) {
                    final Record inputRecord = RecordUtils.getKeyValueRecord("", line);
                    enqueue(inputRecord);
                } else {
                    String[] kvLine = line.split(keyValueSeparator);
                    final Record inputRecord = RecordUtils.getKeyValueRecord(kvLine[0], kvLine[1]);
                    enqueue(inputRecord);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void enqueue(InputStream inputStream) {
        enqueue(null, inputStream);
    }


    @Override
    public boolean removeProperty(PropertyDescriptor descriptor) {
        return context.removeProperty(descriptor.getName());
    }

    @Override
    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        return context.setProperty(propertyName, propertyValue);
    }

    @Override
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final String value) {
        return context.setProperty(descriptor.getName(), value);
    }

    @Override
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final AllowableValue value) {
        return context.setProperty(descriptor.getName(), value.getValue());
    }


    @Override
    public void assertAllInputRecordsProcessed() {
        assertTrue(inputRecordsQueue.isEmpty());
    }

    @Override
    public void assertOutputRecordsCount(int count) {
        assertTrue("expected output record count was " + count + " but is currently " +
                outputRecordsList.size(), outputRecordsList.size() == count);
    }

    @Override
    public void assertOutputErrorCount(int count) {
        long errorCount = outputRecordsList.stream().filter(r -> r.hasField(FieldDictionary.RECORD_ERRORS)).count();
        assertTrue("expected output error record count was " + count + " but is currently " +
                errorCount, errorCount == count);
    }

    @Override
    public void assertAllRecordsContainAttribute(String attributeName) {

    }

    @Override
    public void assertAllRecords(RecordValidator validator) {
        outputRecordsList.forEach(validator::assertRecord);
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

    @Override
    public void clearQueues() {
        outputRecordsList.clear();
    }

    @Override
    public List<MockRecord> getOutputRecords() {
        return outputRecordsList
                .stream()
                .map(MockRecord::new)
                .collect(Collectors.toList());
    }


    @Override
    public List<MockRecord> getErrorRecords() {
        return getOutputRecords()
                .stream()
                .filter(r -> r.hasField(FieldDictionary.RECORD_ERRORS))
                .collect(Collectors.toList());
    }
}



