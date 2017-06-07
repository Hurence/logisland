
package com.hurence.logisland.util.runner;


import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ControllerServiceConfiguration {

    private final ControllerService service;
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private String annotationData;
    private Map<PropertyDescriptor, String> properties = new HashMap<>();

    public ControllerServiceConfiguration(final ControllerService service) {
        this.service = service;
    }

    public ControllerService getService() {
        return service;
    }

    public void setEnabled(final boolean enabled) {

        this.enabled.set(enabled);
    }

    public boolean isEnabled() {
        return this.enabled.get();
    }

    public void setProperties(final Map<PropertyDescriptor, String> props) {
        this.properties = new HashMap<>(props);
    }

    public String getProperty(final PropertyDescriptor descriptor) {
        final String value = properties.get(descriptor);
        if (value == null) {
            return descriptor.getDefaultValue();
        } else {
            return value;
        }
    }

    public void setAnnotationData(final String annotationData) {
        this.annotationData = annotationData;
    }

    public String getAnnotationData() {
        return annotationData;
    }

    public Map<PropertyDescriptor, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }
}
