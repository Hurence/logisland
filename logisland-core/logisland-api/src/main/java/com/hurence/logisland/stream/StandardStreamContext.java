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
package com.hurence.logisland.stream;


import com.hurence.logisland.component.*;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.processor.ProcessContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StandardStreamContext extends AbstractConfiguredComponent implements StreamContext {

    private final List<ProcessContext> processContexts = new ArrayList<>();

    public StandardStreamContext(final RecordStream recordStream, final String id) {
        super(recordStream, id);
    }

    @Override
    public RecordStream getStream() {
        return (RecordStream) component;
    }

    @Override
    public Collection<ProcessContext> getProcessContexts() {
        return processContexts;
    }

    @Override
    public void addProcessContext(ProcessContext processContext) {
        processContexts.add(processContext);
    }

    private ControllerServiceLookup controllerServiceLookup;

    @Override
    public void setControllerServiceLookup(ControllerServiceLookup controllerServiceLookup) throws InitializationException {
        this.controllerServiceLookup = controllerServiceLookup;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() throws InitializationException {
        return this.controllerServiceLookup;
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

        return PropertyValueFactory.getInstance(descriptor, propValue, controllerServiceLookup);
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue);
    }

    @Override
    public boolean isValid(boolean strictCheck) {
        boolean streamValid = super.isValid(strictCheck);
        if (!streamValid) {
            getLogger().error("conf of stream is not valid !");
        }
        boolean processorsValid = true;
        for (final ProcessContext processContext : processContexts) {
            if (!processContext.isValid(strictCheck)) {
                processorsValid = false;
            }
        }
        return streamValid && processorsValid;
    }

}
