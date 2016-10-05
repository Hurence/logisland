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


import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.ValidationResult;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.record.Record;

import java.io.InputStream;

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
     * Remova all records from output queue
     */
    void clearOutpuRecords();

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
}
