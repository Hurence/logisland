
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

public class MockControllerServiceInitializationContext extends MockControllerServiceLookup
        implements ControllerServiceInitializationContext, ControllerServiceLookup {

    private final ControllerService service;
    private final ComponentLog logger;
    private final Map<PropertyDescriptor, String> properties = new HashMap<>();
    private final VariableRegistry variableRegistry;

    public MockControllerServiceInitializationContext(final ControllerService controllerService, final Map<String, String> props) {
        this.service = controllerService;
        this.logger = new MockComponentLogger();
        this.variableRegistry = VariableRegistry.EMPTY_REGISTRY;
        for (Map.Entry<String, String> prop: props.entrySet()){
            this.properties.put(service.getPropertyDescriptor(prop.getKey()), prop.getValue());
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
        return null;
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
}
