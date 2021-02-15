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

import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.validator.ValidationResult;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

public interface ConfiguredComponent extends Serializable {

    /**
     * identifier is read-only
     *
     * @return the identifier of the component
     */
    String getIdentifier();

    /**
     * Sets the property with the given name to the given value
     *  @param name the name of the property to update
     * @param value the value to update the property to
     */
    ValidationResult setProperty(String name, String value);

    /**
     * Removes the property and value for the given property name if a
     * descriptor and value exists for the given name. If the property is
     * optional its value might be reset to default or will be removed entirely
     * if was a dynamic property.
     *
     * @param name the property to removeField
     * @return true if removed; false otherwise
     * @throws IllegalArgumentException if the name is null
     */
    boolean removeProperty(String name);

    /**
     * @return map of property/names
     */
    Map<PropertyDescriptor, String> getProperties();

    /**
     * get a property
     *
     * @param property
     * @return
     */
    String getProperty(final PropertyDescriptor property);

    /**
     * @return if configuration is valid
     */
    boolean isValid();

    /**
     *
     * @param strictCheck
     * @return if configuration is valid and if configuration does not contain unsupported properties
     */
    boolean isValid(boolean strictCheck);

    /**
     * @return the any validation errors for this connectable
     */
    Collection<ValidationResult> getValidationErrors();

    /**
     * @return a logger that can be used to log important events in a standard
     * way and generate bulletins when appropriate
     */
    ComponentLog getLogger();
}
