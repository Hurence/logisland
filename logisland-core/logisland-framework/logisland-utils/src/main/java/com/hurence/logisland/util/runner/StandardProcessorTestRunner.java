/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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

import com.hurence.logisland.annotation.lifecycle.OnAdded;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.annotation.lifecycle.OnRemoved;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.processor.StandardValidationContext;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class StandardProcessorTestRunner implements TestRunner {

    private final Processor processor;
    private final MockProcessContext context;
    private final MockVariableRegistry variableRegistry;
    private final MockControllerServiceLookup serviceLookup;

    private final List<Record> inputRecordsQueue;
    private final List<Record> outputRecordsList;

    private static Logger logger = LoggerFactory.getLogger(StandardProcessorTestRunner.class);
    private static final AtomicLong currentId = new AtomicLong(0);

    StandardProcessorTestRunner(final ProcessContext processContext) {
        this(processContext.getProcessor());
    }

    StandardProcessorTestRunner(final Processor processor) {
        this.processor = processor;
        this.inputRecordsQueue = new ArrayList<>();
        this.outputRecordsList = new ArrayList<>();
        this.variableRegistry = new MockVariableRegistry();
        this.serviceLookup = new MockControllerServiceLookup();
        this.context = new MockProcessContext(processor, this.serviceLookup);
    }

    @Override
    public ProcessContext getProcessContext() {
        return context;
    }

    @Override
    public void run() {
        try {
            this.processor.init(context);
        } catch (InitializationException e) {
            throw new RuntimeException(e);
        }
        Collection<Record> outputRecords = processor.process(context, inputRecordsQueue);
        outputRecordsList.addAll(outputRecords);
        inputRecordsQueue.clear();
    }

    @Override
    public void setProcessorIdentifier(String identifier) {
        this.context.setIdentifier(identifier);
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
        Collections.addAll(inputRecordsQueue, records);
    }

    @Override
    public void enqueue(Collection<? extends Record> records) {
        inputRecordsQueue.addAll(records);
    }

    @Override
    public void enqueue(List<String> values) {
        for (final String value : values) {
            enqueue(null, value);
        }
    }

    @Override
    public void enqueue(final String key, String value) {
        final Record record = RecordUtils.getKeyValueRecord(key, value);
        enqueue(record);
    }

    @Override
    public void enqueue(byte[] key, byte[] value) {
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
    public boolean removeProperty(String propertyName) {
        return context.removeProperty(propertyName);
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
        long recordsCount =
                outputRecordsList.stream().filter(r -> !r.hasField(FieldDictionary.RECORD_ERRORS)).count();
        assertTrue("expected output record count was " + count + " but is currently " +recordsCount, recordsCount == count);
    }

    @Override
    public void assertOutputRecordsIncludingErrorsCount(int count) {
        long recordsCount = outputRecordsList.stream().count();
        assertTrue("expected total output record (including errors) count was " + count + " but is currently " +recordsCount, recordsCount == count);
    }

    @Override
    public void assertOutputErrorCount(int count) {
        long errorCount = outputRecordsList.stream().filter(r -> r.hasField(FieldDictionary.RECORD_ERRORS)).count();
        assertTrue("expected output error record count was " + count + " but is currently " +
                errorCount, errorCount == count);
    }

    @Override
    public void assertAllOutputRecords(RecordValidator validator) {
        outputRecordsList.forEach(validator::assertRecord);
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


    @Override
    public void disableControllerService(final ControllerService service) {
        final MockControllerService configuration = serviceLookup.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (!configuration.isEnabled()) {
            throw new IllegalStateException("Controller service " + service + " cannot be disabled because it is not enabled");
        }

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnDisabled.class, service);
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to disable Controller Service " + service + " due to " + e);
        }

        configuration.setEnabled(false);
    }

    @Override
    public void enableControllerService(final ControllerService service) {
        final MockControllerService configuration = serviceLookup.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Cannot enable Controller Service " + service + " because it is not disabled");
        }

        assertValid(service);

        try {
         //   final ControllerServiceInitializationContext configContext = new MockConfigurationContext(service, configuration.getProperties(), context, variableRegistry);
            final MockControllerServiceInitializationContext initContext =
                    new MockControllerServiceInitializationContext(requireNonNull(service), requireNonNull(service.getIdentifier()), this.serviceLookup);

            for(Map.Entry<PropertyDescriptor, String> entry : configuration.getProperties().entrySet()) {
                initContext.setProperty(entry.getKey().getName(), entry.getValue());
            }

            ReflectionUtils.invokeMethodsWithAnnotation(OnEnabled.class, service, initContext);

            try {
                service.initialize(initContext);
            } catch (InitializationException ex) {
                logger.error("Error during initialization", ex);
            }
        } catch (final InvocationTargetException ite) {
            ite.getCause().printStackTrace();
            Assert.fail("Failed to enable Controller Service " + service + " due to " + ite.getCause());
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to enable Controller Service " + service + " due to " + e);
        }

        configuration.setEnabled(true);
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        final MockControllerService configuration = serviceLookup.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        return configuration.isEnabled();
    }

    @Override
    public void removeControllerService(final ControllerService service) {
        disableControllerService(service);

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnRemoved.class, service);
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to remove Controller Service " + service + " due to " + e);
        }

        serviceLookup.removeControllerService(service);
    }

    @Override
    public void addControllerService(final String identifier, final ControllerService service) throws InitializationException {
        addControllerService(identifier, service, new HashMap<String, String>());
    }

    @Override
    public void addControllerService(final String identifier, final ControllerService service, final Map<String, String> properties) throws InitializationException {
        context.addControllerService(identifier, service, null);

        final MockControllerServiceInitializationContext initContext =
                new MockControllerServiceInitializationContext(requireNonNull(service), requireNonNull(identifier), this.serviceLookup);

        for(Map.Entry<String, String> entry : properties.entrySet()) {
            initContext.setProperty(entry.getKey(), entry.getValue());
        }

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, service);
        } catch (final InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
            throw new InitializationException(e);
        }

        try {
            //needed to associate identifier to service see AbstractControllerService
            //If it fails we ignore it as the unique purpose is to intialize identifier of service
            service.initialize(initContext);
        } catch (Exception ex) {
            //logger.error("Error during initialization", ex);
        }
        //Needed to save given properties for next use
        //WARNING ! Must be after service.initialize(initContext) so that service identifier is correctly set
        for(Map.Entry<String, String> entry : properties.entrySet()) {
            setProperty(service, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        return this.serviceLookup.getControllerService(identifier);
    }

    @Override
    public void assertNotValid(final ControllerService service) {

        final ValidationContext validationContext = new StandardValidationContext(this.getConfigOfService(service).getProperties());
        final Collection<ValidationResult> results = this.serviceLookup.getControllerService(service.getIdentifier()).validate(validationContext);

        for (final ValidationResult result : results) {
            if (!result.isValid()) {
                return;
            }
        }
        Assert.fail("Expected Controller Service " + service + " to be invalid but it is valid");
    }

    @Override
    public void assertValid(final ControllerService service) {
        final ValidationContext validationContext = new StandardValidationContext(this.getConfigOfService(service).getProperties());
        final Collection<ValidationResult> results = this.serviceLookup.getControllerService(service.getIdentifier()).validate(validationContext);

        for (final ValidationResult result : results) {
            if (!result.isValid()) {
                Assert.fail("Expected Controller Service to be valid but it is invalid due to: " + result.toString());
            }
        }
    }

    /**
     *
     * @param service
     * @return config of service if it exists and disabled
     */
    private MockControllerService getConfigToUpdate(final ControllerService service) {
        final MockControllerService configuration = getConfigOfService(service);

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Controller service " + service + " cannot be modified because it is not disabled");
        }

        return configuration;
    }

    /**
     *
     * @param service
     * @return config of service if it exists
     */
    private MockControllerService getConfigOfService(final ControllerService service) {
        final MockControllerService configuration = this.serviceLookup.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        return configuration;
    }

    @Override
    public ValidationResult setProperty(final ControllerService service, final PropertyDescriptor property, final AllowableValue value) {
        return setProperty(service, property, value.getValue());
    }

    @Override
    public ValidationResult setProperty(final ControllerService service, final PropertyDescriptor property, final String value) {


        final MockControllerService configuration = getConfigToUpdate(service);
        final Map<PropertyDescriptor, String> curProps = configuration.getProperties();
        final Map<PropertyDescriptor, String> updatedProps = new HashMap<>(curProps);

        final ValidationResult validationResult = property.validate(value/*, validationContext*/);

        final String oldValue = updatedProps.get(property);
        updatedProps.put(property, value);
        configuration.setProperties(updatedProps);

        if ((value == null && oldValue != null) || (value != null && !value.equals(oldValue))) {
            service.onPropertyModified(property, oldValue, value);
        }

        return validationResult;
    }

    @Override
    public ValidationResult setProperty(final ControllerService service, final String propertyName, final String value) {
        final PropertyDescriptor descriptor = service.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return new ValidationResult.Builder()
                    .input(propertyName)
                    .explanation(propertyName + " is not a known Property for Controller Service " + service)
                    .subject("Invalid property")
                    .valid(false)
                    .build();
        }
        return setProperty(service, descriptor, value);
    }

}



