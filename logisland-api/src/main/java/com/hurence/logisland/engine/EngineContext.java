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
package com.hurence.logisland.engine;


import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;

import java.util.Map;

/**
 * <p>
 * Provides a bridge between a Processor and the Framework
 * </p>
 * <p/>
 * <p>
 * <b>Note: </b>Implementations of this interface are NOT necessarily
 * thread-safe.
 * </p>
 */
public interface EngineContext {

    /**
     * Retrieves the current value set for the given descriptor, if a value is
     * set - else uses the descriptor to determine the appropriate default value
     *
     * @param descriptor to lookup the value of
     * @return the property value of the given descriptor
     */
    PropertyValue getProperty(PropertyDescriptor descriptor);

    /**
     * Retrieves the current value set for the given descriptor, if a value is
     * set - else uses the descriptor to determine the appropriate default value
     *
     * @param propertyName of the property to lookup the value for
     * @return property value as retrieved by property name
     */
    PropertyValue getProperty(String propertyName);

    /**
     * Creates and returns a {@link PropertyValue} object that can be used for
     * evaluating the value of the given String
     *
     * @param rawValue the raw input before any property evaluation has occurred
     * @return a {@link PropertyValue} object that can be used for
     * evaluating the value of the given String
     */
    PropertyValue newPropertyValue(String rawValue);


    /**
     * @return a Map of all PropertyDescriptors to their configured values. This
     * Map may or may not be modifiable, but modifying its values will not
     * change the values of the processor's properties
     */
    Map<PropertyDescriptor, String> getProperties();


    /**
     * @return the configured name of this processor
     */
    String getName();
}
