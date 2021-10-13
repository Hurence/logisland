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
package com.hurence.logisland.processor;


import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.component.StandardPropertyValue;
import com.hurence.logisland.validator.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StandardConfiguration implements Configuration {

    private final Map<PropertyDescriptor, String> properties;
    private final Map<String, Boolean> expressionLanguageSupported;

    public StandardConfiguration(final Map<PropertyDescriptor, String> properties) {
        this.properties = new HashMap<>(properties);
        expressionLanguageSupported = new HashMap<>(properties.size());
        for (final PropertyDescriptor descriptor : properties.keySet()) {
            expressionLanguageSupported.put(descriptor.getName(), descriptor.isExpressionLanguageSupported());
        }
    }

    @Override
    public PropertyValue getPropertyValue(final PropertyDescriptor property) {
        final String configuredValue = properties.get(property);
        return new StandardPropertyValue(configuredValue == null ? property.getDefaultValue() : configuredValue);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }
}
