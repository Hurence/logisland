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
package com.hurence.logisland.controller;


import com.hurence.logisland.component.*;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.ProcessingEngine;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.stream.StreamContext;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StandardControllerServiceContext extends AbstractConfiguredComponent implements ComponentContext, ControllerServiceInitializationContext {



    private final ComponentLog logger;



    public StandardControllerServiceContext(final ControllerService controllerService, final String identifier) {
        super(controllerService, identifier);
        this.logger =  new StandardComponentLogger(identifier, StandardControllerServiceContext.class);
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
    public void verifyModifiable() throws IllegalStateException {

    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return null;
    }

    @Override
    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public String getKerberosServicePrincipal() {
        return null;
    }

    @Override
    public File getKerberosServiceKeytab() {
        return null;
    }

    @Override
    public File getKerberosConfigurationFile() {
        return null;
    }
}
