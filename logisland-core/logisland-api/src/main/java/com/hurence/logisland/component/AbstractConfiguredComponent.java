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
import com.hurence.logisland.processor.StandardConfiguration;
import com.hurence.logisland.validator.Configuration;
import com.hurence.logisland.validator.ValidationResult;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public abstract class AbstractConfiguredComponent implements ConfigurableComponent, ConfiguredComponent {

    protected final String id;
    protected final ConfigurableComponent component;
    protected final ComponentLog componentLog;

    private final Lock lock = new ReentrantLock();
    protected final ConcurrentMap<PropertyDescriptor, String> properties = new ConcurrentHashMap<>();

    public AbstractConfiguredComponent(final ConfigurableComponent component, final String id) {
        this.id = id;
        this.component = component;
        this.componentLog = new StandardComponentLogger(id, this);
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public ValidationResult setProperty(final String name, final String value) {
        if (null == name || null == value) {
            throw new IllegalArgumentException();
        }

        lock.lock();
        ValidationResult result =null;
        try {
            final PropertyDescriptor descriptor = getPropertyDescriptor(name);
            result = descriptor.validate(value);
            if (!result.isValid()) {
                //throw new IllegalArgumentException(result.toString());
                getLogger().warn(result.toString());
            }

            final String oldValue = properties.put(descriptor, value);
            if (!value.equals(oldValue)) {
                try {
                    onPropertyModified(descriptor, oldValue, value);
                } catch (final Exception e) {
                    // nothing really to do here...
                }
            }
        } catch (final Exception e) {
            // nothing really to do here...
        } finally {
            lock.unlock();
            return result;
        }
    }

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
    @Override
    public boolean removeProperty(final String name) {
        if (null == name) {
            throw new IllegalArgumentException();
        }

        lock.lock();
        try {
            final PropertyDescriptor descriptor = getPropertyDescriptor(name);
            String value = null;
            if (!descriptor.isRequired() && (value = properties.remove(descriptor)) != null) {


                try {
                    onPropertyModified(descriptor, value, null);
                } catch (final Exception e) {
                    // nothing really to do here...
                }

                return true;
            }
        } catch (final Exception e) {
            // nothing really to do here...
        } finally {
            lock.unlock();
        }

        return false;
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {

        final List<PropertyDescriptor> supported = getPropertyDescriptors();
        if (supported == null || supported.isEmpty()) {
            return Collections.unmodifiableMap(properties);
        } else {
            final Map<PropertyDescriptor, String> props = new LinkedHashMap<>();
            for (final PropertyDescriptor descriptor : supported) {
                props.put(descriptor, null);
            }
            props.putAll(properties);
            return props;
        }

    }



    @Override
    public String getProperty(final PropertyDescriptor property) {
        return properties.get(property);
    }

    @Override
    public int hashCode() {
        return 273171 * id.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof ConfiguredComponent)) {
            return false;
        }

        final ConfiguredComponent other = (ConfiguredComponent) obj;
        return id.equals(other.getIdentifier());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[id=" + getIdentifier() + ", component=" + component.getClass().getSimpleName() + "]";
    }


    @Override
    public PropertyDescriptor getPropertyDescriptor(final String name) {
        return component.getPropertyDescriptor(name);
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        component.onPropertyModified(descriptor, oldValue, newValue);
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return component.getPropertyDescriptors();
    }

    @Override
    public boolean isValid() {
        return isValid(false);
    }

    @Override
    public boolean isValid(boolean strictCheck) {
        final Collection<ValidationResult> validationResults = getValidationErrors();
        boolean isValid = true;
        for (final ValidationResult result : validationResults) {
            //TODO tolerate unsupported properties or no depending on strictCheck
            if (!result.isValid()) {
                getLogger().error("invalid property {}", new Object[]{result.getExplanation()});
                isValid = false;
            }
        }
        if (!isValid) {
            if (component instanceof AbstractConfigurableComponent) {
                AbstractConfigurableComponent abstractComp = (AbstractConfigurableComponent) component;
                List<PropertyDescriptor> descriptors = abstractComp.getSupportedPropertyDescriptors();
                getLogger().info("Here the supported properties for this component:");
                descriptors.forEach(desc -> {
                    getLogger().info("{}", new Object[]{desc});
                });
            }
        }
        return isValid;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        return validate(new StandardConfiguration(getProperties()));
    }

    @Override
    public Collection<ValidationResult> validate(final Configuration context) {
        return component.validate(context);
    }

    @Override
    public ComponentLog getLogger() {
        return componentLog;
    }


}
