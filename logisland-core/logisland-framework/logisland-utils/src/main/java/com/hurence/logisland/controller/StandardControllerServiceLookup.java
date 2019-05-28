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

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * a controller service lookup is initialized with config and  will dynamically load service instances as needed
 */
public class StandardControllerServiceLookup implements ControllerServiceLookup, Serializable {

    private final Map<String, ControllerService> controllerServiceMap = new ConcurrentHashMap<>();

    private final Collection<ControllerServiceConfiguration> configurations;

    private static final Logger logger = LoggerFactory.getLogger(StandardControllerServiceLookup.class);


    private static final AtomicLong currentId = new AtomicLong(0);

    public StandardControllerServiceLookup(Collection<ControllerServiceConfiguration> configurations) {

        this.configurations = configurations;


    }

    @Override
    public synchronized ControllerService getControllerService(String serviceIdentifier) {

        // check if the service has been loaded
        if (!controllerServiceMap.containsKey(serviceIdentifier)) {

            // lazy load the controller service
            configurations.forEach(conf -> {

                if (serviceIdentifier.equals(conf.getControllerService())) {
                    try {


                        ControllerService service = ComponentFactory.loadComponent(conf.getComponent());
                        logger.info("loading controller service {}", new Object[]{conf.getComponent()});
                        ControllerServiceInitializationContext context = new StandardControllerServiceContext(service, conf.getControllerService());
                        Map<String, String> properties = conf.getConfiguration();
                        properties.keySet().forEach(name -> context.setProperty(name, properties.get(name)));

                        service.initialize(context);
                        controllerServiceMap.put(conf.getControllerService(), service);
                        logger.info("service initialization complete {}", new Object[]{service});
                    } catch (IllegalArgumentException | ClassNotFoundException e) {
                        logger.error("unable to load class {} : {} ", new Object[]{conf, e.toString()});
                    } catch (InitializationException e) {
                        logger.error("unable to initialize class {} : {} ", new Object[]{conf, e.toString()});
                    }
                }


            });

            // now retry finding the service
            if (!controllerServiceMap.containsKey(serviceIdentifier)) {
                logger.error("service {} is not available, exiting", new Object[]{serviceIdentifier});
                System.exit(-1);
            }
        }

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
    public Set<String> getControllerServiceIdentifiers(Class<? extends ControllerService> serviceType) throws
            IllegalArgumentException {
        return null;
    }
}
