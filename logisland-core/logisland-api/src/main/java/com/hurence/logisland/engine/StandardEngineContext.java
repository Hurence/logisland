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
package com.hurence.logisland.engine;


import com.hurence.logisland.component.AbstractConfiguredComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.component.StandardPropertyValue;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.stream.StreamContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StandardEngineContext extends AbstractConfiguredComponent implements EngineContext {


    private final List<StreamContext> streamContexts = new ArrayList<>();
    private final List<ControllerServiceInitializationContext> controllerServiceContext = new ArrayList<>();

    public StandardEngineContext(final ProcessingEngine engine, final String id) {
        super(engine, id);
    }


    @Override
    public Collection<StreamContext> getStreamContexts() {
        return streamContexts;
    }

    @Override
    public void addStreamContext(StreamContext streamContext) {
        streamContexts.add(streamContext);
    }

    @Override
    public ProcessingEngine getEngine() {
        return (ProcessingEngine) component;
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

        return new StandardPropertyValue(propValue);
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue);
    }

    @Override
    public boolean isValid(boolean strictCheck) {
        boolean engineValid = super.isValid(strictCheck);
        if (!engineValid) {
            getLogger().error("Conf of engine is not valid !");
        }
        boolean streamsValid = true;
        for (final StreamContext streamContext : streamContexts) {
            if (!streamContext.isValid(strictCheck)) {
                streamsValid = false;
            }
        }
        boolean servicesValid = true;
        for (final ControllerServiceInitializationContext serviceContext : controllerServiceContext) {
            if (!serviceContext.isValid(strictCheck)) {
                servicesValid = false;
            }
        }
        return engineValid && streamsValid && servicesValid;
    }

    @Override
    public Collection<ControllerServiceInitializationContext> getControllerServiceContexts() {
        return controllerServiceContext;
    }

    @Override
    public void addControllerServiceContext(ControllerServiceInitializationContext config) {
        controllerServiceContext.add(config);
    }

}
