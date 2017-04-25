/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.component;


//import com.hurence.logisland.utils.time.FormatUtils;

import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.registry.VariableRegistry;

public class StandardPropertyValue implements PropertyValue {

    private final String rawValue;
    private final ControllerServiceLookup serviceLookup;
    private final VariableRegistry variableRegistry;

    public StandardPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup) {
        this(rawValue, serviceLookup, VariableRegistry.EMPTY_REGISTRY);
    }

    /**
     * Constructs a new StandardPropertyValue with the given value & service
     * lookup and indicates whether or not the rawValue contains any NiFi
     * Expressions. If it is unknown whether or not the value contains any NiFi
     * Expressions, the
     * {@link #StandardPropertyValue(String, ControllerServiceLookup, VariableRegistry)}
     * constructor should be used or <code>true</code> should be passed.
     *
     * @param rawValue value
     * @param serviceLookup lookup
     * @param variableRegistry variableRegistry
     */
    public StandardPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup,
                                 final VariableRegistry variableRegistry) {
        this.rawValue = rawValue;
        this.serviceLookup = serviceLookup;
        this.variableRegistry = variableRegistry;
    }

    /**
     * Constructs a new StandardPropertyValue with the given value. If it is unknown whether or not the value
     *
     * @param rawValue value
     */
    public StandardPropertyValue(final String rawValue) {

        this(rawValue, null, VariableRegistry.EMPTY_REGISTRY);
    }

    public String getRawValue() {
        return rawValue;
    }

    @Override
    public String asString() {
        return rawValue;
    }

    @Override
    public Integer asInteger() {
        return (rawValue == null) ? null : Integer.parseInt(rawValue.trim());
    }

    @Override
    public Long asLong() {
        return (rawValue == null) ? null : Long.parseLong(rawValue.trim());
    }

    @Override
    public Boolean asBoolean() {
        return (rawValue == null) ? null : Boolean.parseBoolean(rawValue.trim());
    }

    @Override
    public Float asFloat() {
        return (rawValue == null) ? null : Float.parseFloat(rawValue.trim());
    }

    @Override
    public Double asDouble() {
        return (rawValue == null) ? null : Double.parseDouble(rawValue.trim());
    }

  /*  @Override
    public Long asTimePeriod(final TimeUnit timeUnit) {
        return (rawValue == null) ? null : FormatUtils.getTimeDuration(rawValue.trim(), timeUnit);
    }*/


    @Override
    public boolean isSet() {
        return rawValue != null;
    }

    @Override
    public ControllerService asControllerService() {
        if (rawValue == null || rawValue.equals("") || serviceLookup == null) {
            return null;
        }

        return serviceLookup.getControllerService(rawValue);
    }

    @Override
    public <T extends ControllerService> T asControllerService(final Class<T> serviceType) throws IllegalArgumentException {
        if (!serviceType.isInterface()) {
            throw new IllegalArgumentException("ControllerServices may be referenced only via their interfaces; " + serviceType + " is not an interface");
        }
        if (rawValue == null || rawValue.equals("") || serviceLookup == null) {
            return null;
        }

        final ControllerService service = serviceLookup.getControllerService(rawValue);
        if (service == null) {
            return null;
        }
        if (serviceType.isAssignableFrom(service.getClass())) {
            return serviceType.cast(service);
        }
        throw new IllegalArgumentException("Controller Service with identifier " + rawValue + " is of type " + service.getClass() + " and cannot be cast to " + serviceType);
    }
}
