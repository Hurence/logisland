package com.hurence.logisland.util.runner;



import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.controller.ConfigurationContext;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.registry.VariableRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MockConfigurationContext implements ConfigurationContext {

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
    public Map<PropertyDescriptor, String> getProperties() {
        return new HashMap<>(this.properties);
    }

    private PropertyDescriptor getActualDescriptor(final PropertyDescriptor property) {
        if (service == null) {
            return property;
        }

        final PropertyDescriptor resolved = service.getPropertyDescriptor(property.getName());
        return resolved == null ? property : resolved;
    }

    @Override
    public String getSchedulingPeriod() {
        return "0 secs";
    }

    @Override
    public Long getSchedulingPeriod(final TimeUnit timeUnit) {
        return 0L;
    }
}
