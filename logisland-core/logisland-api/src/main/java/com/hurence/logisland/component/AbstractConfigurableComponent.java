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
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.validator.Configuration;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class AbstractConfigurableComponent implements ConfigurableComponent {

    protected String identifier = "";
    protected ComponentLog componentLogger;
    private static Logger logger = LoggerFactory.getLogger(AbstractConfigurableComponent.class);

    @Override
    public String getIdentifier() {
        return identifier;
    }


    @Deprecated
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

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
    protected Collection<ValidationResult> customValidate(Configuration context){
        return Collections.emptySet();
    }


    //ValidationContext même a la confusion finalement ce n'est qu'un wrapper de Map<PropertyDescrisptor, String>
    //==> interface Properties à la place ? ou juste Map<PropertyDescrisptor, String> ?
    @Override
    public final Collection<ValidationResult> validate(final Configuration context) {
        // goes through supported properties
        final Collection<ValidationResult> results = new ArrayList<>();
        final List<PropertyDescriptor> supportedDescriptors = getSupportedPropertyDescriptors();
        if (null != supportedDescriptors) {
            for (final PropertyDescriptor descriptor : supportedDescriptors) {
                String value = context.getPropertyValue(descriptor).asString();
                if (value == null) {
                    value = descriptor.getDefaultValue();
                }
                if (value == null && descriptor.isRequired()) {
                    results.add(new ValidationResult.Builder().valid(false).input(null).subject(descriptor.getName()).explanation(descriptor.getName() + " is required").build());
                    continue;
                } else if (value == null) {
                    continue;
                }

                final ValidationResult result = descriptor.validate(value);
                if (!result.isValid()) {
                    results.add(result);
                }
            }
        }

        // validate any dynamic properties
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            final String value = entry.getValue();

            if (supportedDescriptors != null && !supportedDescriptors.contains(descriptor)) {
                final ValidationResult result = descriptor.validate(value);
                if (!result.isValid()) {
                    results.add(result);
                }
            }
        }

        // only run customValidate if regular validation is successful. This allows Processor developers to not have to check
        // if values are null or invalid so that they can focus only on the interaction between the properties, etc.
        if (results.isEmpty()) {
            final Collection<ValidationResult> customResults = customValidate(context);
            if (null != customResults) {
                for (final ValidationResult result : customResults) {
                    if (!result.isValid()) {
                        results.add(result);
                    }
                }
            }
        }

        // log issues
        if (!results.isEmpty()) {
            for (ValidationResult result:results) {
                logger.warn(result.toString());
            }
        }

        return results;
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
        getLogger().info("property {} value changed from {} to {}", new Object[]{descriptor.getName(), oldValue, newValue});
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

    /**
     * Provides subclasses the ability to perform initialization logic
     */
    public void init(final ComponentContext context) throws InitializationException {
        identifier = context.getIdentifier();
        componentLogger = context.getLogger();
    }

    /**
     * @return the logger that has been provided to the component by the
     * framework in its init method
     */
    protected ComponentLog getLogger() {
        if (componentLogger==null) {
            componentLogger = new StandardComponentLogger(this.getIdentifier(), this);
        }
        return componentLogger;
    }


    @Override
    public final List<PropertyDescriptor> getPropertyDescriptors() {
        final List<PropertyDescriptor> supported = getSupportedPropertyDescriptors();
        return supported == null ? Collections.<PropertyDescriptor>emptyList() : new ArrayList<>(supported);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[id=" + getIdentifier() + "]";
    }

}
