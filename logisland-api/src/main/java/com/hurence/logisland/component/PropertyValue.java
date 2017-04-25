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



import com.hurence.logisland.controller.ControllerService;

import java.io.Serializable;

/**
 * <p>
 * A PropertyValue provides a mechanism whereby the currently configured value
 * of a processor property can be obtained in different forms.
 * </p>
 */
public interface PropertyValue extends Serializable {

    /**
     * @return the raw property value as a string
     */
    public Object getRawValue();


    /**
     * @return an String representation of the property value, or
     * <code>null</code> if not set
     */
    public String asString();

    /**
     * @return an integer representation of the property value, or
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    public Integer asInteger();

    /**
     * @return a Long representation of the property value, or <code>null</code>
     * if not set
     * @throws NumberFormatException if not able to parse
     */
    public Long asLong();

    /**
     * @return a Boolean representation of the property value, or
     * <code>null</code> if not set
     */
    public Boolean asBoolean();

    /**
     * @return a Float representation of the property value, or
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    public Float asFloat();

    /**
     * @return a Double representation of the property value, of
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    public Double asDouble();

    /**
     * @param timeUnit specifies the TimeUnit to convert the time duration into
     * @return a Long value representing the value of the configured time period
     * in terms of the specified TimeUnit; if the property is not set, returns
     * <code>null</code>
     */
   // public Long asTimePeriod(TimeUnit timeUnit);



    /**
     * @return <code>true</code> if the user has configured a value, or if the
     * {@link PropertyDescriptor} for the associated property has a default
     * value, <code>false</code> otherwise
     */
    public boolean isSet();


    /**
     * @return the ControllerService whose identifier is the raw value of
     * <code>this</code>, or <code>null</code> if either the value is not set or
     * the value does not identify a ControllerService
     */
    ControllerService asControllerService();

    /**
     * @param <T> the generic type of the controller service
     * @param serviceType the class of the Controller Service
     * @return the ControllerService whose identifier is the raw value of the
     * <code>this</code>, or <code>null</code> if either the value is not set or
     * the value does not identify a ControllerService. The object returned by
     * this method is explicitly cast to type specified, if the type specified
     * is valid. Otherwise, throws an IllegalArgumentException
     *
     * @throws IllegalArgumentException if the value of <code>this</code> points
     * to a ControllerService but that service is not of type
     * <code>serviceType</code> or if <code>serviceType</code> references a
     * class that is not an interface
     */
    <T extends ControllerService> T asControllerService(Class<T> serviceType) throws IllegalArgumentException;


}
