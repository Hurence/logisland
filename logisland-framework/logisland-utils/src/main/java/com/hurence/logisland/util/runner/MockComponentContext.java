package com.hurence.logisland.util.runner;

import com.hurence.logisland.component.*;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.processor.StandardValidationContext;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.validator.ValidationResult;

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * Created by gregoire on 09/06/17.
 */
public class MockComponentContext implements ComponentContext {

    private ControllerServiceLookup controllerServiceLookup;
    private ConfigurableComponent component;
    private Map<PropertyDescriptor, String> properties;
    private final VariableRegistry variableRegistry;

    public MockComponentContext(ConfigurableComponent component, Map<PropertyDescriptor, String> properties,
                                VariableRegistry variableRegistry,
                                ControllerServiceLookup controllerServiceLookup) {
        this.component = component;
        this.properties = properties;
        this.variableRegistry = variableRegistry;
        this.controllerServiceLookup = controllerServiceLookup;
    }


    @Override
    public String getIdentifier() {
        return component.getIdentifier();
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public boolean removeProperty(String name){
        if (null == name) {
            throw new IllegalArgumentException();
        }

        try {
            final PropertyDescriptor descriptor = component.getPropertyDescriptor(name);
            String value = null;
            if (!descriptor.isRequired() && (value = properties.remove(descriptor)) != null) {
                try {
                    component.onPropertyModified(descriptor, value, null);
                } catch (final Exception e) {
                    // nothing really to do here...
                }

                return true;
            }
        } catch (final Exception e) {
            // nothing really to do here...
        }

        return false;
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

        return new MockPropertyValue(propValue, controllerServiceLookup, variableRegistry, descriptor);
    }


    @Override
    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        return setProperty(new PropertyDescriptor.Builder().name(propertyName).build(), propertyValue);
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


    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue);
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
    public String getName() {
        return "";
    }
}
