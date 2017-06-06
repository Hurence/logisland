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


import com.hurence.logisland.annotation.lifecycle.OnAdded;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.annotation.lifecycle.OnRemoved;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.record.Record;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface TestRunner {

    /**
     * @return the {@link Processor} for which this <code>TestRunner</code> is
     * configured
     */
    Processor getProcessor();


    /**
     * @return the {@Link ProcessContext} that this <code>TestRunner</code> will
     * use
     */
    ProcessContext getProcessContext();

    /**
     * Performs the operation.
     */
    void run();


    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, Assert.fail() is called, causing the
     * unit test to fail
     *
     * @param propertyName  name
     * @param propertyValue value
     * @return result
     */
    ValidationResult setProperty(String propertyName, String propertyValue);

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, Assert.fail() is called, causing the
     * unit test to fail
     *
     * @param descriptor descriptor
     * @param value      value
     * @return result
     */
    ValidationResult setProperty(PropertyDescriptor descriptor, String value);

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, Assert.fail() is called, causing the
     * unit test to fail
     *
     * @param descriptor descriptor
     * @param value      allowable valu
     * @return result
     */
    ValidationResult setProperty(PropertyDescriptor descriptor, AllowableValue value);


    /**
     * Asserts that all Records that were processed in the input queue
     * to <code>count</code>
     */
    void assertAllInputRecordsProcessed();

    /**
     * Asserts that output Records size is equal
     * to <code>count</code>
     */
    void assertOutputRecordsCount(int count);


    /**
     * Asserts that the size of output Records in error is equal
     * to <code>count</code>
     */
    void assertOutputErrorCount(int count);

    /**
     * Remova all records from output queue
     */
    void clearQueues();


    /**
     * Retrieve output queue
     */
    List<MockRecord> getOutputRecords();

    /**
     * Retrieve output error queue
     */
    List<MockRecord> getErrorRecords();


    /**
     * Asserts that all Records that were transferred contain the given
     * attribute.
     *
     * @param attributeName attribute to look for
     */
    void assertAllRecordsContainAttribute(String attributeName);


    /**
     * Asserts that all Records that were transferred are compliant with the
     * given validator.
     *
     * @param validator validator to use
     */
    void assertAllRecords(RecordValidator validator);


    /**
     * Assert that the currently configured set of properties/annotation data
     * are valid
     */
    void assertValid();

    /**
     * Assert that the currently configured set of properties/annotation data
     * are NOT valid
     */
    void assertNotValid();


    /**
     * Enqueues the given Records into the Processor's input queue
     *
     * @param Records to enqueue
     */
    void enqueue(Record... Records);


    /**
     * Enqueues the given Records into the Processor's input queue
     *
     * @param records to enqueue
     */
    void enqueue(Collection<Record> records);


    /**
     * Enqueues the given Records into the Processor's input queue
     *
     * @param values to enqueue
     */
    void enqueue(List<String> values);

    /**
     * Enqueues the given list of string as Records
     *
     * @param values
     */
    void enqueue(String[] values);


    /**
     * Creates a Record with the content set to the given string (in UTF-8 format), with no attributes,
     * and adds this Record to the Processor's Input Queue.
     * <p>
     * the key will be set as empty string if keyValueSeparator is empty
     *
     * @param key   the key/value separator of the message
     * @param value the key of the message
     */
    void enqueue(byte[] key, byte[] value);

    /**
     * Creates a Record with the content set to the given string (in UTF-8 format), with no attributes,
     * and adds this Record to the Processor's Input Queue.
     * <p>
     * the key will be set as empty string if keyValueSeparator is empty
     *
     * @param key   the key/value separator of the message
     * @param value the key of the message
     */
    void enqueue(String key, String value);

    /**
     * Creates a Record with the content set to the given inputuStrem (in UTF-8 format), with no attributes,
     * and adds this Record to the Processor's Input Queue
     * <p>
     * the key will be set as empty string if keyValueSeparator is empty
     *
     * @param keyValueSeparator the key of all messages message
     * @param inputStream       the inputstream containing all the messages lines
     */
    void enqueue(String keyValueSeparator, InputStream inputStream);

    /**
     * Creates a Record with the content set to the given inputuStrem (in UTF-8 format), with no attributes,
     * and adds this Record to the Processor's Input Queue
     *
     * @param inputStream the inputstream containing all the messages lines
     */
    void enqueue(InputStream inputStream);

    /**
     * Removes the {@link PropertyDescriptor} from the {@link ProcessContext},
     * effectively setting its value to null, or the property's default value, if it has one.
     *
     * @param descriptor of property to remove
     * @return <code>true</code> if removed, <code>false</code> if the property was not set
     */
    boolean removeProperty(PropertyDescriptor descriptor);


    /**
     * Sets the value of the variable with the given name to be the given value. This exposes the variable
     * for use by the Expression Language.
     *
     * @param name  the name of the variable to set
     * @param value the value of the variable
     * @throws NullPointerException if either the name or the value is null
     */
    void setVariable(String name, String value);

    /**
     * Returns the current value of the variable with the given name
     *
     * @param name the name of the variable whose value should be returned.
     * @return the current value of the variable with the given name or <code>null</code> if no value is currently set
     * @throws NullPointerException if the name is null
     */
    String getVariableValue(String name);

    /**
     * Removes the variable with the given name from this Test Runner, if it is set.
     *
     * @param name the name of the variable to remove
     * @return the value that was set for the variable, or <code>null</code> if the variable was not set
     * @throws NullPointerException if the name is null
     */
    String removeVariable(String name);



    /**
     * @param identifier of controller service
     * @return the {@link ControllerService} that is registered with the given
     *         identifier, or <code>null</code> if no Controller Service exists with the
     *         given identifier
     */
    ControllerService getControllerService(String identifier);

    /**
     * Assert that the currently configured set of properties/annotation data
     * are valid for the given Controller Service.
     *
     * @param service the service to validate
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(String, ControllerService)} or
     *             {@link #addControllerService(String, ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     */
    void assertValid(ControllerService service);

    /**
     * Assert that the currently configured set of properties/annotation data
     * are NOT valid for the given Controller Service.
     *
     * @param service the service to validate
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(String, ControllerService)} or
     *             {@link #addControllerService(String, ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     *
     */
    void assertNotValid(ControllerService service);

    /**
     * Adds the given {@link ControllerService} to this TestRunner so that the
     * configured Processor can access it using the given
     * <code>identifier</code>. The ControllerService is not expected to be
     * initialized, as the framework will create the appropriate
     * {@link ControllerServiceInitializationContext ControllerServiceInitializationContext}
     * and initialize the ControllerService with no specified properties.
     *
     * This will call any method on the given Controller Service that is
     * annotated with the
     * {@link OnAdded @OnAdded} annotation.
     *
     * @param service the service
     * @throws InitializationException ie
     */
    void addControllerService(ControllerService service) throws InitializationException;

    /**
     * Adds the given {@link ControllerService} to this TestRunner so that the
     * configured Processor can access it using the given
     * <code>identifier</code>. The ControllerService is not expected to be
     * initialized, as the framework will create the appropriate
     * {@link ControllerServiceInitializationContext ControllerServiceInitializationContext}
     * and initialize the ControllerService with the given properties.
     *
     * This will call any method on the given Controller Service that is
     * annotated with the
     * {@link OnAdded @OnAdded} annotation.
     *
     * @param service the service
     * @param properties service properties
     * @throws InitializationException ie
     */
    void addControllerService(ControllerService service, Map<String, String> properties) throws InitializationException;

    /**
     * <p>
     * Marks the Controller Service as enabled so that it can be used by other
     * components.
     * </p>
     *
     * <p>
     * This method will result in calling any method in the Controller Service
     * that is annotated with the
     * {@link OnEnabled @OnEnabled}
     * annotation.
     * </p>
     *
     * @param service the service to enable
     */
    void enableControllerService(ControllerService service);

    /**
     * <p>
     * Marks the Controller Service as disabled so that it cannot be used by
     * other components.
     * </p>
     *
     * <p>
     * This method will result in calling any method in the Controller Service
     * that is annotated with the
     * {@link OnDisabled @OnDisabled}
     * annotation.
     * </p>
     *
     * @param service the service to disable
     */
    void disableControllerService(ControllerService service);

    /**
     * @param service the service
     * @return {@code true} if the given Controller Service is enabled,
     *         {@code false} if it is disabled
     *
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(ControllerService)} or
     *             {@link #addControllerService(ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     */
    boolean isControllerServiceEnabled(ControllerService service);

    /**
     * <p>
     * Removes the Controller Service from the TestRunner. This will call any
     * method on the ControllerService that is annotated with the
     * {@link OnRemoved @OnRemoved}
     * annotation.
     * </p>
     *
     * @param service the service
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(ControllerService)} or
     *             {@link #addControllerService(ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     *
     */
    void removeControllerService(ControllerService service);


    /**
     * Sets the given property on the given ControllerService
     *
     * @param service to modify
     * @param property to modify
     * @param value value to use
     * @return result
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(ControllerService)} or
     *             {@link #addControllerService(ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     *
     */
    ValidationResult setProperty(ControllerService service, PropertyDescriptor property, String value);

    /**
     * Sets the given property on the given ControllerService
     *
     * @param service to modify
     * @param property to modify
     * @param value value to use
     * @return result
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(ControllerService)} or
     *             {@link #addControllerService(ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     *
     */
    ValidationResult setProperty(ControllerService service, PropertyDescriptor property, AllowableValue value);

    /**
     * Sets the property with the given name on the given ControllerService
     *
     * @param service to modify
     * @param propertyName to modify
     * @param value value to use
     * @return result
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(ControllerService)} or
     *             {@link #addControllerService(ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     *
     */
    ValidationResult setProperty(ControllerService service, String propertyName, String value);
}
