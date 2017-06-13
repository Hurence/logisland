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

import com.hurence.logisland.annotation.lifecycle.OnAdded;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.util.runner.ReflectionUtils;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;


/**
 * a controller service lookup is initialized with config and  will dynamically load service instances as needed
 */
public class StandardControllerServiceLookup implements ControllerServiceLookup, Serializable {

    private final Map<String, ControllerService> controllerServiceMap = new ConcurrentHashMap<>();

    private static ComponentLog logger = new StandardComponentLogger("standardControllerServiceLookup", StandardControllerServiceLookup.class);


    private static final AtomicLong currentId = new AtomicLong(0);

    public StandardControllerServiceLookup(Collection<ControllerServiceConfiguration> configurations) {


        configurations.forEach(controllerServiceConfiguration -> {

            try {
                AbstractControllerService service = (AbstractControllerService) Class.forName(controllerServiceConfiguration.getComponent()).newInstance();

                ControllerServiceInitializationContext context = new StandardControllerServiceContext(service, Long.toString(currentId.incrementAndGet()));
                Map<String,String> properties = controllerServiceConfiguration.getConfiguration();
                properties.keySet().forEach( name -> context.setProperty(name, properties.get(name)));


                controllerServiceMap.put(controllerServiceConfiguration.getControllerService(), service);
                service.initialize(context);
            } catch (IllegalAccessException |
                    IllegalArgumentException |
                    ClassNotFoundException |
                    InstantiationException e) {
                logger.error("unable to load class {} : {} ", new Object[]{controllerServiceConfiguration, e.toString()});
            } catch (InitializationException e) {
                logger.error("unable to initialize class {} : {} ", new Object[]{controllerServiceConfiguration, e.toString()});
            }

        });

    }

    @Override
    public ControllerService getControllerService(String serviceIdentifier) {

        logger.debug("getting controller service {}", new Object[]{serviceIdentifier});
        return controllerServiceMap.get(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabled(String serviceIdentifier) {
        return false;
    }

    @Override
    public boolean isControllerServiceEnabling(String serviceIdentifier) {
        return false;
    }

    @Override
    public boolean isControllerServiceEnabled(ControllerService service) {
        return false;
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(Class<? extends ControllerService> serviceType) throws IllegalArgumentException {
        return null;
    }

    @Override
    public String getControllerServiceName(String serviceIdentifier) {
        return null;
    }
}
