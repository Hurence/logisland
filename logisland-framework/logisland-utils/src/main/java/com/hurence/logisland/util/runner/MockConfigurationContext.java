package com.hurence.logisland.util.runner;



import com.hurence.logisland.component.AbstractConfiguredComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.component.StandardPropertyValue;
import com.hurence.logisland.controller.ConfigurationContext;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.validator.ValidationResult;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class MockConfigurationContext extends AbstractConfiguredComponent implements ConfigurationContext {


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
        super(null, "");
        this.service = service;
        this.properties.putAll(properties);
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
    public PropertyValue newPropertyValue(String rawValue) {
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
    public void verifyModifiable() throws IllegalStateException {

    }
}
