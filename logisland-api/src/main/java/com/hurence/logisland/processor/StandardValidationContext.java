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
package com.hurence.logisland.processor;



import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.component.StandardPropertyValue;
import com.hurence.logisland.component.ValidationContext;

public class StandardValidationContext implements ValidationContext {

    private final Map<PropertyDescriptor, String> properties;
    private final Map<String, Boolean> expressionLanguageSupported;
    private final String annotationData;
    private final Set<String> serviceIdentifiersToNotValidate;
    private final String groupId;
    private final String componentId;

    public StandardValidationContext(final Map<PropertyDescriptor, String> properties,
                                     final String annotationData, final String groupId, final String componentId) {
        this(Collections.<String> emptySet(), properties, annotationData, groupId, componentId);
    }

    public StandardValidationContext(
            final Set<String> serviceIdentifiersToNotValidate,
            final Map<PropertyDescriptor, String> properties,
            final String annotationData,
            final String groupId,
            final String componentId) {
        this.properties = new HashMap<>(properties);
        this.annotationData = annotationData;
        this.serviceIdentifiersToNotValidate = serviceIdentifiersToNotValidate;
        this.groupId = groupId;
        this.componentId = componentId;

        expressionLanguageSupported = new HashMap<>(properties.size());
        for (final PropertyDescriptor descriptor : properties.keySet()) {
            expressionLanguageSupported.put(descriptor.getName(), descriptor.isExpressionLanguageSupported());
        }
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue);
    }



    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final String configuredValue = properties.get(property);
        return new StandardPropertyValue(configuredValue == null ? property.getDefaultValue() : configuredValue);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public String getAnnotationData() {
        return annotationData;
    }




    @Override
    public boolean isExpressionLanguagePresent(final String value) {
       /* if (value == null) {
            return false;
        }

        final List<Range> elRanges = Query.extractExpressionRanges(value);
        return (elRanges != null && !elRanges.isEmpty());*/
        return false;
    }

    @Override
    public boolean isExpressionLanguageSupported(final String propertyName) {
        final Boolean supported = expressionLanguageSupported.get(propertyName);
        return Boolean.TRUE.equals(supported);
    }

    @Override
    public String getProcessGroupIdentifier() {
        return groupId;
    }
}
