/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hurence.logisland.util.runner;


import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.component.StandardPropertyValue;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.MockComponentLogger;
import com.hurence.logisland.validator.ValidationResult;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class MockControllerServiceInitializationContext extends MockControllerServiceLookup
        implements ControllerServiceInitializationContext, ControllerServiceLookup {

    private final String identifier;
    private final ComponentLog logger;
    private final Map<PropertyDescriptor, String> properties = new HashMap<>();

    public MockControllerServiceInitializationContext(final ControllerService controllerService, final String identifier) {
        this.identifier = identifier;
        this.logger = new MockComponentLogger();
    }


    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public void setName(String name) {

    }


    @Override
    public String getProperty(PropertyDescriptor property) {
        return null;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        return null;
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        return null;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this;
    }

    @Override
    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public String getKerberosServicePrincipal() {
        return null; //this needs to be wired in.
    }

    @Override
    public File getKerberosServiceKeytab() {
        return null; //this needs to be wired in.
    }

    @Override
    public File getKerberosConfigurationFile() {
        return null; //this needs to be wired in.
    }


    @Override
    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        properties.put(new PropertyDescriptor.Builder().name(propertyName).build(), propertyValue);
        return new ValidationResult.Builder().valid(true).build();
    }

    @Override
    public boolean removeProperty(String name) {
        return false;
    }

    @Override
    public PropertyValue getPropertyValue(final PropertyDescriptor descriptor) {
        return getPropertyValue(descriptor.getName());
    }

    @Override
    public PropertyValue getPropertyValue(final String propertyName) {


       return new MockPropertyValue(properties.get(new PropertyDescriptor.Builder().name(propertyName).build()));
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return properties;
    }

    @Override
    public String getName() {
        return null;
    }
}
