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
package com.hurence.logisland.util.runner;


import com.hurence.logisland.component.*;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.util.FormatUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MockPropertyValue implements PropertyValue {
    private final String rawValue;
    private final Boolean expectExpressions;
    private final ControllerServiceLookup serviceLookup;
    private final PropertyDescriptor propertyDescriptor;
    private final PropertyValue stdPropValue;
    private final VariableRegistry variableRegistry;
    private boolean expressionsEvaluated = false;

    public MockPropertyValue(final String rawValue) {
        this(rawValue, null);
    }

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup) {
        this(rawValue, serviceLookup, VariableRegistry.EMPTY_REGISTRY, null);
    }

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final VariableRegistry variableRegistry) {
        this(rawValue, serviceLookup, variableRegistry, null);
    }

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, VariableRegistry variableRegistry, final PropertyDescriptor propertyDescriptor) {
        this(rawValue, serviceLookup, propertyDescriptor, false, variableRegistry);
    }

    private MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final PropertyDescriptor propertyDescriptor, final boolean alreadyEvaluated,
                              final VariableRegistry variableRegistry) {
        //this.stdPropValue = new StandardPropertyValue(rawValue);
        this.stdPropValue = PropertyValueFactory.getInstance(propertyDescriptor, rawValue, serviceLookup);
        this.rawValue = rawValue;
        this.serviceLookup = serviceLookup;
        this.expectExpressions = propertyDescriptor == null ? null : propertyDescriptor.isExpressionLanguageSupported();
        this.propertyDescriptor = propertyDescriptor;
        this.expressionsEvaluated = alreadyEvaluated;
        this.variableRegistry = variableRegistry;
    }


    private void ensureExpressionsEvaluated() {
        if (Boolean.TRUE.equals(expectExpressions) && !expressionsEvaluated) {
            throw new IllegalStateException("Attempting to retrieve value of " + propertyDescriptor
                    + " without first evaluating Expressions, even though the PropertyDescriptor indicates "
                    + "that the Expression Language is Supported. If you realize that this is the case and do not want "
                    + "this error to occur, it can be disabled by calling TestRunner.setValidateExpressionUsage(false)");
        }
    }

    @Override
    public Object getRawValue() {
        return rawValue;
    }

    @Override
    public String asString() {
        return rawValue;
    }

    @Override
    public Integer asInteger() {
        ensureExpressionsEvaluated();
        return stdPropValue.asInteger();
    }

    @Override
    public Record asRecord() {
        return (getRawValue() == null) ? null : new StandardRecord()
                .setStringField(FieldDictionary.RECORD_VALUE,rawValue);
    }
    @Override
    public Long asLong() {
        ensureExpressionsEvaluated();
        return stdPropValue.asLong();
    }

    @Override
    public Boolean asBoolean() {
        ensureExpressionsEvaluated();
        return stdPropValue.asBoolean();
    }

    @Override
    public Float asFloat() {
        ensureExpressionsEvaluated();
        return stdPropValue.asFloat();
    }

    @Override
    public Double asDouble() {
        ensureExpressionsEvaluated();
        return stdPropValue.asDouble();
    }

    @Override
    public Long asTimePeriod(final TimeUnit timeUnit) {
        return (rawValue == null) ? null : FormatUtils.getTimeDuration(rawValue.trim(), timeUnit);
    }

    private void markEvaluated() {
        if (Boolean.FALSE.equals(expectExpressions)) {
            throw new IllegalStateException("Attempting to Evaluate Expressions but " + propertyDescriptor
                    + " indicates that the Expression Language is not supported. If you realize that this is the case and do not want "
                    + "this error to occur, it can be disabled by calling TestRunner.setValidateExpressionUsage(false)");
        }
        expressionsEvaluated = true;
    }





    @Override
    public ControllerService asControllerService() {
        ensureExpressionsEvaluated();
        if (rawValue == null || rawValue.equals("")) {
            return null;
        }

        return serviceLookup.getControllerService(rawValue);
    }



    @Override
    public boolean isSet() {
        return rawValue != null;
    }

    @Override
    public String toString() {
        return asString();
    }

    @Override
    public PropertyValue evaluate(Record record) {

        markEvaluated();

        if (stdPropValue instanceof InterpretedPropertyValue) {
            return stdPropValue.evaluate(record);
        }

        return stdPropValue;
    }
}
