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

import com.hurence.logisland.component.*;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.processor.StandardValidationContext;
import com.hurence.logisland.processor.state.StateManager;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.validator.ValidationResult;

import java.util.*;

import static java.util.Objects.requireNonNull;

public class MockProcessContext implements ProcessContext, ControllerServiceLookup {

    private String identifier;
    private final ConfigurableComponent component;
    private final Map<PropertyDescriptor, String> properties = new HashMap<>();
    private final VariableRegistry variableRegistry;
    private final MockControllerServiceLookup serviceLookup;


    /**
     * Creates a new MockProcessContext for the given Processor
     *
     * @param component being mocked
     * @param variableRegistry variableRegistry
     */
    public MockProcessContext(final ConfigurableComponent component,
                              final VariableRegistry variableRegistry) {
        this(component, variableRegistry, new MockControllerServiceLookup());
    }

    private MockProcessContext(final ConfigurableComponent component,
                               final VariableRegistry variableRegistry,
                               MockControllerServiceLookup serviceLookup) {
        this.component = Objects.requireNonNull(component);
        this.variableRegistry = variableRegistry;
        this.identifier = component.getIdentifier() == null ? "mock_processor" : component.getIdentifier();
        this.serviceLookup = serviceLookup;
    }

    /**
     * Creates a new MockProcessContext for the given Processor
     *
     * @param component being mocked
     */
    public MockProcessContext(final Processor component) {
        this(component, VariableRegistry.EMPTY_REGISTRY);
    }

    /**
     * Creates a new MockProcessContext for the given Processor
     *
     * @param component being mocked
     */
    public MockProcessContext(final Processor component, MockControllerServiceLookup serviceLookup) {
        this(component, VariableRegistry.EMPTY_REGISTRY, serviceLookup);
    }

    public MockProcessContext(final ControllerService component, final MockProcessContext context, final VariableRegistry variableRegistry) {
        this(component, variableRegistry);

        try {
            final Map<PropertyDescriptor, String> props = context.getProperties();
            properties.putAll(props);
            this.serviceLookup.addControllerServices(context.serviceLookup);
        } catch (IllegalArgumentException e) {
            // do nothing...the service is being loaded
        }
    }

    @Override
    public PropertyValue getPropertyValue(final PropertyDescriptor descriptor) {
        return getPropertyValue(descriptor.getName());
    }

    @Override
    public PropertyValue getPropertyValue(final String propertyName) {
        final PropertyDescriptor descriptor = component.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return null;
        }

        final String setPropertyValue = properties.get(descriptor);
        final String propValue = (setPropertyValue == null) ? descriptor.getDefaultValue() : setPropertyValue;

        return PropertyValueFactory.getInstance(descriptor, propValue, this.serviceLookup);
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue);
    }


    @Override
    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        return setProperty(new PropertyDescriptor.Builder().name(propertyName).build(), propertyValue);
    }

    @Override
    public boolean removeProperty(String name) {
        final PropertyDescriptor fullyPopulatedDescriptor = component.getPropertyDescriptor(name);
        String value = null;

        if ((value = properties.remove(fullyPopulatedDescriptor)) != null) {
            if (!value.equals(fullyPopulatedDescriptor.getDefaultValue())) {
                component.onPropertyModified(fullyPopulatedDescriptor, value, null);
            }

            return true;
        }
        return false;
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
        final PropertyDescriptor fullyPopulatedDescriptor = component.getPropertyDescriptor(descriptor.getName());

        final ValidationResult result = fullyPopulatedDescriptor.validate(value);
        String oldValue = properties.put(fullyPopulatedDescriptor, value);
        if (oldValue == null) {
            oldValue = fullyPopulatedDescriptor.getDefaultValue();
        }
        if ((value == null && oldValue != null) || (value != null && !value.equals(oldValue))) {
            component.onPropertyModified(fullyPopulatedDescriptor, oldValue, value);
        }

        return result;
    }

    public boolean removeProperty(final PropertyDescriptor descriptor) {
        Objects.requireNonNull(descriptor);
        return removeProperty(descriptor.getName());
    }


    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        final List<PropertyDescriptor> supported = component.getPropertyDescriptors();
        if (supported == null || supported.isEmpty()) {
            return Collections.unmodifiableMap(properties);
        } else {
            final Map<PropertyDescriptor, String> props = new LinkedHashMap<>();
            for (final PropertyDescriptor descriptor : supported) {
                props.put(descriptor, null);
            }
            props.putAll(properties);
            return props;
        }
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
        return component.validate(new StandardValidationContext(properties));
    }

    public boolean isValid() {
        for (final ValidationResult result : validate()) {
            if (!result.isValid()) {
                getLogger().warn("invalid property {}", new Object[]{result.getExplanation()});
                return false;
            }
        }

        return true;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        return validate();
    }


    @Override
    public String getIdentifier() {
        return this.identifier;
    }


    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public ComponentLog getLogger() {
        return new StandardComponentLogger(this.getIdentifier(), this.component);
    }

    @Override
    public void setControllerServiceLookup(ControllerServiceLookup controllerServiceLookup) throws InitializationException {

    }

    @Override
    public Processor getProcessor() {
        return (Processor) component;
    }

    @Override
    public StateManager getStateManager() {
        return null;
    }

    @Override
    public void yield() {

    }


    public void addControllerService(final String serviceIdentifier,
                                     final ControllerService controllerService,
                                     final String annotationData) {
        requireNonNull(controllerService);
        this.serviceLookup.addControllerService(controllerService, serviceIdentifier)
                .setAnnotationData(annotationData);
    }


    @Override
    public ControllerService getControllerService(String serviceIdentifier) {
        return this.serviceLookup.getControllerService(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabled(String serviceIdentifier) {
        return this.serviceLookup.isControllerServiceEnabled(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabling(String serviceIdentifier) {
        return this.serviceLookup.isControllerServiceEnabling(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabled(ControllerService service) {
        return this.serviceLookup.isControllerServiceEnabled(service);
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(Class<? extends ControllerService> serviceType) throws IllegalArgumentException {
        return this.serviceLookup.getControllerServiceIdentifiers(serviceType);
    }

    public MockControllerServiceLookup getServiceLookup() {
        return serviceLookup;
    }
}
