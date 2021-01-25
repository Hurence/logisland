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
import com.hurence.logisland.component.StandardPropertyValue;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.validator.ValidationResult;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MockControllerServiceInitializationContext implements ControllerServiceInitializationContext {

    private final String identifier;
    private final ComponentLog logger;
    private final Map<PropertyDescriptor, String> properties;
    private final ControllerService controllerService;
    private final MockControllerServiceLookup serviceLookup;

    public MockControllerServiceInitializationContext(final ControllerService controllerService,
                                                      final String identifier,
                                                      MockControllerServiceLookup serviceLookup) {
        this.identifier = identifier;
        this.controllerService = controllerService;
        this.logger = new StandardComponentLogger(identifier, controllerService);
        this.serviceLookup = serviceLookup;
        this.properties = new HashMap<>();
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getProperty(PropertyDescriptor property) {
        return properties.getOrDefault(property, getPropertyValue(property).asString());
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
    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        PropertyDescriptor descriptor = controllerService.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            logger.warn("property '" + propertyName + "' does not exist on " + identifier);
            properties.put(new PropertyDescriptor.Builder().name(propertyName).build(), propertyValue);
            return new ValidationResult.Builder().valid(true).build();
        } else {
            properties.put(descriptor, propertyValue);
            return descriptor.validate(propertyValue);
        }
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
        PropertyDescriptor descriptor = controllerService.getPropertyDescriptor(propertyName);
        if (descriptor == null)
            throw new IllegalArgumentException(String.format("there is no such property '{}' in service '{}'", propertyName, controllerService.getIdentifier()));
        if (properties.containsKey(descriptor)) {
            return new MockPropertyValue(properties.get(descriptor), serviceLookup, VariableRegistry.EMPTY_REGISTRY, descriptor);
        } else {
            return new MockPropertyValue(descriptor.getDefaultValue(), serviceLookup, VariableRegistry.EMPTY_REGISTRY, descriptor);
        }
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return properties;
    }

}
