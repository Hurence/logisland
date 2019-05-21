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
package com.hurence.logisland.component;


import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
    Object getRawValue();


    /**
     * @return an String representation of the property value, or
     * <code>null</code> if not set
     */
    String asString();

    /**
     * @return an String representation of the property value, or
     * <code>null</code> if not set
     */
    default Optional<String> asStringOpt() {
        return Optional.ofNullable(asString());
    }

    /**
     * @return an integer representation of the property value, or
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    Integer asInteger();


    /**
     * @return a byte[] representation of the property value, of
     * <code>null</code> if not set
     * @throws IllegalArgumentException if not able to parse
     */
    byte[] asBytes();

    /**
     * @return a Record representation of the property value, or
     * <code>null</code> if not set
     */
    Record asRecord();


    /**
     * @return a Long representation of the property value, or <code>null</code>
     * if not set
     * @throws NumberFormatException if not able to parse
     */
    Long asLong();

    /**
     * @return a Boolean representation of the property value, or
     * <code>null</code> if not set
     */
    Boolean asBoolean();

    /**
     * @return a Float representation of the property value, or
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    Float asFloat();

    /**
     * @return a Double representation of the property value, of
     * <code>null</code> if not set
     * @throws NumberFormatException if not able to parse
     */
    Double asDouble();

    /**
     * @param timeUnit specifies the TimeUnit to convert the time duration into
     * @return a Long value representing the value of the configured time period
     * in terms of the specified TimeUnit; if the property is not set, returns
     * <code>null</code>
     */
    Long asTimePeriod(TimeUnit timeUnit);



    /**
     * @return <code>true</code> if the user has configured a value, or if the
     * {@link PropertyDescriptor} for the associated property has a default
     * value, <code>false</code> otherwise
     */
    boolean isSet();


    /**
     * @return the ControllerService whose identifier is the raw value of
     * <code>this</code>, or <code>null</code> if either the value is not set or
     * the value does not identify a ControllerService
     */
    ControllerService asControllerService();


    /**
     * In the case of PropertyDescriptors that do support expression language, the fill method allows to
     * obtain a PropertyValue that is filled in with record content.
     * @param record
     * @return
     */
    PropertyValue evaluate(Record record);

    /**
     * In the case of PropertyDescriptors that do support expression language, the fill method allows to
     * obtain a PropertyValue that is filled in with map content.
     *
     * @param attributes a Map of attributes that the Expression language can reference.
     *
     * @return a PropertyValue with the new value
     */
    default PropertyValue evaluate(Map<String, String> attributes) {
        Record record = new StandardRecord("from_map");
        record.setStringFields(attributes);
        return evaluate(record);
    }
}
