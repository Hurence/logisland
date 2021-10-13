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


import com.hurence.logisland.validator.Configuration;
import com.hurence.logisland.validator.ValidationResult;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public interface ConfigurableComponent extends Serializable{

    /**
     * Validates a set of properties, returning ValidationResults for any
     * invalid properties. All defined properties will be validated. If they are
     * not included in the in the purposed configuration, the default value will
     * be used.
     *
     * @param context of validation
     * @return Collection of validation result objects for any invalid findings
     * only. If the collection is empty then the component is valid. Guaranteed
     * non-null
     */
    Collection<ValidationResult> validate(Configuration context);

    /**
     * @param name to lookup the descriptor
     * @return the PropertyDescriptor with the given name, if it exists;
     * otherwise, returns <code>null</code>
     */
    PropertyDescriptor getPropertyDescriptor(String name);

    /**
     * Hook method allowing subclasses to eagerly react to a configuration
     * change for the given property descriptor. This method will be invoked
     * regardless of property validity. As an alternative to using this method,
     * a component may simply getField the latest value whenever it needs it and if
     * necessary lazily evaluate it. Any throwable that escapes this method will
     * simply be ignored.
     *
     *
     * @param descriptor the descriptor for the property being modified
     * @param oldValue the value that was previously set, or null if no value
     *            was previously set for this property
     * @param newValue the new property value or if null indicates the property
     *            was removed
     */
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue);

    /**
     * Returns a {@link List} of all {@link PropertyDescriptor}s that this
     * component supports.
     *
     * @return PropertyDescriptor objects this component currently supports
     */
    List<PropertyDescriptor> getPropertyDescriptors();

    /**
     * @return the unique identifier that the framework assigned to this
     * component
     */
    String getIdentifier();
}
