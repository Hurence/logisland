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



import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.controller.ConfigurationContext;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.MockComponentLogger;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.validator.ValidationResult;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MockConfigurationContext implements ControllerServiceInitializationContext {

    private final ComponentLog logger;
    private final Map<PropertyDescriptor, String> properties;
    private final ControllerServiceLookup serviceLookup;
    private final ControllerService service;
    private final VariableRegistry variableRegistry;
    private final MockComponentContext componentContext;


    public MockConfigurationContext(final Map<PropertyDescriptor, String> properties,
                                    final ControllerServiceLookup serviceLookup) {
        this(null, properties, serviceLookup, VariableRegistry.EMPTY_REGISTRY);
    }

    public MockConfigurationContext(final Map<PropertyDescriptor, String> properties,
                                    final ControllerServiceLookup serviceLookup,
                                    final VariableRegistry variableRegistry) {
        this(null, properties, serviceLookup, variableRegistry);
    }

    public MockConfigurationContext(final ControllerService service,
                                    final Map<PropertyDescriptor, String> properties,
                                    final ControllerServiceLookup serviceLookup,
                                    final VariableRegistry variableRegistry) {
        this.service = service;
        this.properties = properties;
        this.serviceLookup = serviceLookup;
        this.variableRegistry = variableRegistry;
        this.componentContext = new MockComponentContext(service, properties, variableRegistry, serviceLookup);
        this.logger = new MockComponentLogger();
    }


    @Override
    public String getIdentifier() {
        return componentContext.getIdentifier();
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this.serviceLookup;
    }

    @Override
    public ComponentLog getLogger() {
        return null;
    }

    @Override
    public String getKerberosServicePrincipal() {
        return null;
    }

    @Override
    public File getKerberosServiceKeytab() {
        return null;
    }

    @Override
    public File getKerberosConfigurationFile() {
        return null;
    }

    @Override
    public PropertyValue getPropertyValue(PropertyDescriptor descriptor) {
        return componentContext.getPropertyValue(descriptor);
    }

    @Override
    public PropertyValue getPropertyValue(String propertyName) {
        return componentContext.getPropertyValue(propertyName);
    }

    @Override
    public ValidationResult setProperty(String name, String value) {
        return componentContext.setProperty(name, value);
    }

    @Override
    public boolean removeProperty(String name) {
        return componentContext.removeProperty(name);
    }

    @Override
    public PropertyValue newPropertyValue(String rawValue) {
        return componentContext.newPropertyValue(rawValue);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return componentContext.getProperties();
    }

    @Override
    public String getProperty(PropertyDescriptor property) {
        return componentContext.getProperty(property);
    }

    @Override
    public boolean isValid() {
        return componentContext.isValid();
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        return componentContext.getValidationErrors();
    }

    @Override
    public String getName() {
        return componentContext.getName();
    }

    @Override
    public void setName(String name) {
        componentContext.setName(name);
    }
}
