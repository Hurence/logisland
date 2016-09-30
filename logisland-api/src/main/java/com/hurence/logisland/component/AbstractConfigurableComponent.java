/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.component;

import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.validator.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public abstract class AbstractConfigurableComponent implements ConfigurableComponent {

    private static Logger logger = LoggerFactory.getLogger(AbstractProcessor.class);

    /**
     * Allows subclasses to perform their own validation on the already set
     * properties. Since each property is validated as it is set this allows
     * validation of groups of properties together. Default return is an empty
     * set.
     * <p>
     * This method will be called only when it has been determined that all
     * property getAllFields are valid according to their corresponding
     * PropertyDescriptor's validators.
     *
     * @return Collection of ValidationResult objects that will be added to any
     * other validation findings - may be null
     */
    protected Collection<ValidationResult> customValidate() {
        return Collections.emptySet();
    }

    /**
     * @param descriptorName to lookup the descriptor
     * @return a PropertyDescriptor for the name specified that is fully
     * populated
     */
    @Override
    public final PropertyDescriptor getPropertyDescriptor(final String descriptorName) {
        final PropertyDescriptor specDescriptor = new PropertyDescriptor.Builder().name(descriptorName).build();
        return getPropertyDescriptor(specDescriptor);
    }

    private PropertyDescriptor getPropertyDescriptor(final PropertyDescriptor specDescriptor) {
        PropertyDescriptor descriptor = null;
        //check if property supported
        final List<PropertyDescriptor> supportedDescriptors = getSupportedPropertyDescriptors();
        if (supportedDescriptors != null) {
            for (final PropertyDescriptor desc : supportedDescriptors) { //find actual descriptor
                if (specDescriptor.equals(desc)) {
                    return desc;
                }
            }
        }

        descriptor = getSupportedDynamicPropertyDescriptor(specDescriptor.getName());
        if (descriptor != null && !descriptor.isDynamic()) {
            descriptor = new PropertyDescriptor.Builder().fromPropertyDescriptor(descriptor).dynamic(true).build();
        }

        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().fromPropertyDescriptor(specDescriptor).addValidator(Validator.INVALID).dynamic(true).build();
        }
        return descriptor;
    }


    /**
     * Hook method allowing subclasses to eagerly react to a configuration
     * change for the given property descriptor. As an alternative to using this
     * method a processor may simply getField the latest value whenever it needs it
     * and if necessary lazily evaluate it.
     *
     * @param descriptor of the modified property
     * @param oldValue   non-null property value (previous)
     * @param newValue   the new property value or if null indicates the property
     *                   was removed
     */
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        logger.info("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);
    }

    /**
     * <p>
     * Used to allow subclasses to determine what PropertyDescriptor if any to
     * use when a property is requested for which a descriptor is not already
     * registered. By default this method simply returns a null descriptor. By
     * overriding this method processor implementations can support dynamic
     * properties since this allows them to register properties on demand. It is
     * acceptable for a dynamically generated property to indicate it is
     * required so long as it is understood it is only required once set.
     * Dynamic properties by definition cannot be required until used.</p>
     * <p>
     * <p>
     * This method should be side effect free in the subclasses in terms of how
     * often it is called for a given property name because there is guarantees
     * how often it will be called for a given property name.</p>
     * <p>
     * <p>
     * Default is null.
     *
     * @param propertyDescriptorName used to lookup if any property descriptors exist for that name
     * @return new property descriptor if supported
     */
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return null;
    }

    /**
     * Allows subclasses to register which property descriptor objects are
     * supported.
     *
     * @return PropertyDescriptor objects this processor currently supports
     */
    public abstract List<PropertyDescriptor> getSupportedPropertyDescriptors();
    // {        return Collections.EMPTY_LIST;    }


    /**
     * Provides subclasses the ability to perform initialization logic
     */
    public void init(final ComponentContext context) {
        // Provided for subclasses to override
    }


    @Override
    public final List<PropertyDescriptor> getPropertyDescriptors() {
        final List<PropertyDescriptor> supported = getSupportedPropertyDescriptors();
        return supported == null ? Collections.<PropertyDescriptor>emptyList() : new ArrayList<>(supported);
    }

    @Override
    public String toString() {
        return "AbstractConfigurableComponent{}";
    }

}
