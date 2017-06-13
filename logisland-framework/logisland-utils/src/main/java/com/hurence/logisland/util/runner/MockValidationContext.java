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
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.validator.ValidationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class MockValidationContext implements ValidationContext, ControllerServiceLookup {

    private final MockProcessContext context;
    private final Map<String, Boolean> expressionLanguageSupported;

    private final VariableRegistry variableRegistry;

    public MockValidationContext(final MockProcessContext processContext) {
        this(processContext, VariableRegistry.EMPTY_REGISTRY);
    }

    public MockValidationContext(final MockProcessContext processContext,  final VariableRegistry variableRegistry) {
        this.context = processContext;
        this.variableRegistry = variableRegistry;

        final Map<PropertyDescriptor, String> properties = processContext.getProperties();
        expressionLanguageSupported = new HashMap<>(properties.size());
        for (final PropertyDescriptor descriptor : properties.keySet()) {
            expressionLanguageSupported.put(descriptor.getName(), descriptor.isExpressionLanguageSupported());
        }
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        return context.getControllerService(identifier);
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new MockPropertyValue(rawValue, this, variableRegistry);
    }



    @Override
    public ValidationContext getControllerServiceValidationContext(final ControllerService controllerService) {
        final MockProcessContext serviceProcessContext = new MockProcessContext(controllerService, context, variableRegistry);
        return new MockValidationContext(serviceProcessContext, variableRegistry);
    }

    @Override
    public PropertyValue getPropertyValue(final PropertyDescriptor property) {
        return context.getPropertyValue(property);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return context.getProperties();
    }


    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        return context.getControllerServiceIdentifiers(serviceType);
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this;
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        return context.isControllerServiceEnabled(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return context.isControllerServiceEnabled(service);
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        final ControllerServiceConfiguration configuration = context.getConfiguration(serviceIdentifier);
        return configuration == null ? null : serviceIdentifier;
    }


    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        return context.isControllerServiceEnabling(serviceIdentifier);
    }

    @Override
    public boolean isExpressionLanguagePresent(final String value) {
       /* if (value == null) {
            return false;
        }

        final List<Range> elRanges = Query.extractExpressionRanges(value);
        return (elRanges != null && !elRanges.isEmpty());*/
       return false;
    }

    @Override
    public boolean isExpressionLanguageSupported(final String propertyName) {
        final Boolean supported = expressionLanguageSupported.get(propertyName);
        return Boolean.TRUE.equals(supported);
    }


}
