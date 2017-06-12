/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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

import com.hurence.logisland.component.*;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.MockComponentLogger;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.processor.StandardValidationContext;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.validator.ValidationResult;

import java.util.*;

import static java.util.Objects.requireNonNull;

public class MockProcessContext extends MockControllerServiceLookup implements ControllerServiceLookup, ProcessContext {

    private final ConfigurableComponent component;
    private final Map<PropertyDescriptor, String> properties = new HashMap<>();
    private final VariableRegistry variableRegistry;
    private final MockComponentContext componentContext;
    private final ComponentLog logger;

    /**
     * Creates a new MockProcessContext for the given Processor
     *
     * @param component being mocked
     * @param variableRegistry variableRegistry
     */
    public MockProcessContext(final ConfigurableComponent component, final VariableRegistry variableRegistry) {
        this.component = Objects.requireNonNull(component);
        this.variableRegistry = variableRegistry;
        this.componentContext = new MockComponentContext(component, properties, variableRegistry, this);
        this.logger = new MockComponentLogger();
    }

    /**
     * Creates a new MockProcessContext for the given Processor
     *
     * @param component being mocked
     */
    public MockProcessContext(final Processor component) {
        this(component, VariableRegistry.EMPTY_REGISTRY);
    }

    public MockProcessContext(final ControllerService component, final MockProcessContext context,  final VariableRegistry variableRegistry) {
        this(component, variableRegistry);

        try {
            final Map<PropertyDescriptor, String> props = context.getControllerServiceProperties(component);
            properties.putAll(props);

            super.addControllerServices(context);
        } catch (IllegalArgumentException e) {
            // do nothing...the service is being loaded
        }
    }

    /**
     * Creates a new MockProcessContext for the given ProcessContext
     *
     * @param context
     */
    public MockProcessContext(final ProcessContext context) {
        this(context.getProcessor(), VariableRegistry.EMPTY_REGISTRY);
    }


    Map<PropertyDescriptor, String> getControllerServiceProperties(final ControllerService controllerService) {
        return super.getConfiguration(controllerService.getIdentifier()).getProperties();
    }


    public boolean removeProperty(final PropertyDescriptor descriptor) {
        Objects.requireNonNull(descriptor);
        final PropertyDescriptor fullyPopulatedDescriptor = component.getPropertyDescriptor(descriptor.getName());
        String value = null;

        if ((value = properties.remove(fullyPopulatedDescriptor)) != null) {
            if (!value.equals(fullyPopulatedDescriptor.getDefaultValue())) {
                component.onPropertyModified(fullyPopulatedDescriptor, value, null);
            }

            return true;
        }
        return false;
    }

    @Override
    public void addControllerServiceLookup(ControllerServiceLookup controllerServiceLookup) throws InitializationException {

    }

    @Override
    public Processor getProcessor() {
        return (Processor) component;
    }


    public void addControllerService(final ControllerService controllerService, final Map<PropertyDescriptor, String> properties, final String annotationData) {
        requireNonNull(controllerService);
        final ControllerServiceConfiguration config = addControllerService(controllerService);
        config.setProperties(properties);
        config.setAnnotationData(annotationData);
    }


    @Override
    public String getIdentifier() {
        return componentContext.getIdentifier();
    }

    @Override
    public void setName(String name) {
        componentContext.setName(name);
    }

    @Override
    public boolean removeProperty(String name) {
        return  componentContext.removeProperty(name);
    }

    @Override
    public String getProperty(PropertyDescriptor property) {
        return  componentContext.getProperty(property);
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
    public PropertyValue newPropertyValue(String rawValue) {
        return componentContext.newPropertyValue(rawValue);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return componentContext.getProperties();
    }

    @Override
    public String getName() {
        return componentContext.getName();
    }
}
