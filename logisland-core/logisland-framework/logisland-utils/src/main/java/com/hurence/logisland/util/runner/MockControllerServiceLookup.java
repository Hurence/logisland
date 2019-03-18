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



import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceLookup;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MockControllerServiceLookup implements ControllerServiceLookup {

    private final Map<String, MockControllerService> controllerServiceMap = new ConcurrentHashMap<>();

    public MockControllerService addControllerService(final ControllerService service, final String identifier) {
        final MockControllerService config = new MockControllerService(service);
        controllerServiceMap.put(identifier, config);
        return config;
    }

    public MockControllerService addControllerService(final ControllerService service) {
        return addControllerService(service, service.getIdentifier());
    }

    public void removeControllerService(final ControllerService service) {
        final ControllerService canonical = getControllerService(service.getIdentifier());
        if (canonical == null || canonical != service) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        controllerServiceMap.remove(service.getIdentifier());
    }

    protected void addControllerServices(final MockControllerServiceLookup other) {
        this.controllerServiceMap.putAll(other.controllerServiceMap);
    }

    protected MockControllerService getConfiguration(final String identifier) {
        return controllerServiceMap.get(identifier);
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        final MockControllerService status = controllerServiceMap.get(identifier);
        return (status == null) ? null : status.getService();
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        final MockControllerService status = controllerServiceMap.get(serviceIdentifier);
        if (status == null) {
            throw new IllegalArgumentException("No ControllerService exists with identifier " + serviceIdentifier);
        }

        return status.isEnabled();
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return isControllerServiceEnabled(service.getIdentifier());
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        return false;
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        final Set<String> ids = new HashSet<>();
        for (final Map.Entry<String, MockControllerService> entry : controllerServiceMap.entrySet()) {
            if (serviceType.isAssignableFrom(entry.getValue().getService().getClass())) {
                ids.add(entry.getKey());
            }
        }
        return ids;
    }
}
