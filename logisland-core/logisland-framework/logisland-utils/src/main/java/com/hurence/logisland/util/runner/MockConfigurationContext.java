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
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.validator.ValidationResult;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MockConfigurationContext implements ControllerServiceInitializationContext {

    private final Map<PropertyDescriptor, String> properties;
    private final ControllerServiceLookup serviceLookup;
    private final ControllerService service;
    private final VariableRegistry variableRegistry;

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
    }


    @Override
    public PropertyValue getPropertyValue(PropertyDescriptor property) {
        String value = properties.get(property);
        if (value == null) {
            value = getActualDescriptor(property).getDefaultValue();
        }
        return new MockPropertyValue(value, serviceLookup, variableRegistry);
    }

    @Override
    public PropertyValue getPropertyValue(String propertyName) {
        return null;
    }

    @Override
    public ValidationResult setProperty(String name, String value) {
        return null;
    }

    @Override
    public PropertyValue newPropertyValue(String rawValue) {
        return null;
    }

    @Override
    public String getIdentifier() {
        return null;
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
    public boolean removeProperty(String name) {
        return false;
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return new HashMap<>(this.properties);
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

    private PropertyDescriptor getActualDescriptor(final PropertyDescriptor property) {
        if (service == null) {
            return property;
        }

        final PropertyDescriptor resolved = service.getPropertyDescriptor(property.getName());
        return resolved == null ? property : resolved;
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
}
