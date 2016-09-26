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


import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.components.PropertyValue;
import com.hurence.logisland.components.StandardPropertyValue;

import java.io.Serializable;
import java.util.Map;

public class StandardEngineContext implements EngineContext, Serializable {

    private final StandardEngineInstance engineInstance;


    public StandardEngineContext(final StandardEngineInstance engineInstance) {
        this.engineInstance = engineInstance;

    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor descriptor) {
        return getProperty(descriptor.getName());
    }

    /**
     * <p>
     * Returns the currently configured value for the property with the given name.
     * </p>
     */
    @Override
    public PropertyValue getProperty(final String propertyName) {
        final StreamProcessingEngine engine = engineInstance.getEngine();
        final PropertyDescriptor descriptor = engine.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return null;
        }

        final String setPropertyValue = engineInstance.getProperty(descriptor);
        final String propValue = (setPropertyValue == null) ? descriptor.getDefaultValue() : setPropertyValue;

        return new StandardPropertyValue(propValue);
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue);
    }


    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return engineInstance.getProperties();
    }


    @Override
    public String getName() {
        return engineInstance.getName();
    }
}
