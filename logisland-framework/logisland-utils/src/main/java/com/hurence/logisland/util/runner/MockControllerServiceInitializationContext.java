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
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.MockComponentLogger;
import com.hurence.logisland.processor.StandardValidationContext;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.validator.ValidationResult;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class MockControllerServiceInitializationContext extends MockControllerServiceLookup
        implements ControllerServiceInitializationContext, ControllerServiceLookup {

    private final ControllerService service;
    private final ComponentLog logger;
    private final Map<PropertyDescriptor, String> properties = new HashMap<>();
    private final VariableRegistry variableRegistry;

    public MockControllerServiceInitializationContext(final ControllerService controllerService) {
        this.service = controllerService;
        this.logger = new MockComponentLogger();
        this.variableRegistry = VariableRegistry.EMPTY_REGISTRY;
    }
    public MockControllerServiceInitializationContext(final ControllerService controllerService, final Map<String, String> props) {
       this(controllerService);
        for (Map.Entry<String, String> prop: props.entrySet()){
            this.properties.put(service.getPropertyDescriptor(prop.getKey()), prop.getValue());
        }
    }


    public void setProps(Map<PropertyDescriptor, String> props) {
        for (Map.Entry<PropertyDescriptor, String> prop: props.entrySet()) {
            setProperty(prop.getKey().getName(), prop.getValue());
        }
    }
    @Override
    public String getIdentifier() {
        return service.getIdentifier();
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public boolean removeProperty(String name) {
        return properties.remove(service.getPropertyDescriptor(name)) != null;
    }

    @Override
    public String getProperty(PropertyDescriptor property) {
        return properties.get(property);
    }

    /**
     * Validates the current properties, returning ValidationResults for any
     * invalid properties. All processor defined properties will be validated.
     * If they are not included in the in the purposed configuration, the
     * default value will be used.
     *
     * @return Collection of validation result objects for any invalid findings
     * only. If the collection is empty then the processor is valid. Guaranteed
     * non-null
     */
    public Collection<ValidationResult> validate() {
        return service.validate(new StandardValidationContext(properties));
    }
    @Override
    public boolean isValid() {
        for (final ValidationResult result : validate()) {
            if (!result.isValid()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
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
    public PropertyValue getPropertyValue(PropertyDescriptor descriptor) {
        return getPropertyValue(descriptor.getName());
    }

    @Override
    public PropertyValue getPropertyValue(String propertyName) {
        final PropertyDescriptor descriptor = service.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return null;
        }
        final String setPropertyValue = properties.get(descriptor);
        final String propValue = (setPropertyValue == null) ? descriptor.getDefaultValue() : setPropertyValue;

        return new MockPropertyValue(propValue, this, variableRegistry, descriptor);
    }

    @Override
    public ValidationResult setProperty(String name, String value) {
        return setProperty(new PropertyDescriptor.Builder().name(name).build(), value);
    }

    @Override
    public PropertyValue newPropertyValue(String rawValue) {
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

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, the property value is not updated. In
     * either case, the ValidationResult is returned, indicating whether or not
     * the property is valid
     *
     * @param descriptor of property to modify
     * @param value      new value
     * @return result
     */
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final String value) {
        requireNonNull(descriptor);
        requireNonNull(value, "Cannot set property to null value; if the intent is to remove the property, call removeProperty instead");
        final PropertyDescriptor fullyPopulatedDescriptor = service.getPropertyDescriptor(descriptor.getName());

        final ValidationResult result = fullyPopulatedDescriptor.validate(value);
        String oldValue = properties.put(fullyPopulatedDescriptor, value);
        if (oldValue == null) {
            oldValue = fullyPopulatedDescriptor.getDefaultValue();
        }
        if ((value == null && oldValue != null) || (value != null && !value.equals(oldValue))) {
            service.onPropertyModified(fullyPopulatedDescriptor, oldValue, value);
        }

        return result;
    }
}
