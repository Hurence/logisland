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
package com.hurence.logisland.processor;


import com.hurence.logisland.component.*;
import com.hurence.logisland.controller.ControllerServiceLookup;

public class StandardProcessContext extends AbstractConfiguredComponent implements ProcessContext {

    private ControllerServiceLookup controllerServiceLookup;

    public StandardProcessContext(final Processor processor, final String id) {
        super(processor, id);
    }

    @Override
    public void addControllerServiceLookup(ControllerServiceLookup controllerServiceLookup) throws InitializationException {
        this.controllerServiceLookup = controllerServiceLookup;
    }

    @Override
    public Processor getProcessor() {
        return (Processor) component;
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

        final String setPropertyValue = getProperty(descriptor);
        final String propValue = (setPropertyValue == null) ? descriptor.getDefaultValue() : setPropertyValue;

        return PropertyValueFactory.getInstance(descriptor, setPropertyValue, controllerServiceLookup);
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue);
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {

    }
}
