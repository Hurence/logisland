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
package com.hurence.logisland.record;

import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.util.FormatUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Primitive Types
 * <p>
 * The set of primitive type names is:
 * null: no rawValue
 * boolean: a binary rawValue
 * int: 32-bit signed integer
 * long: 64-bit signed integer
 * float: single precision (32-bit) IEEE 754 floating-point number
 * double: double precision (64-bit) IEEE 754 floating-point number
 * bytes: sequence of 8-bit unsigned bytes
 * string: unicode character sequence
 */
public class Field implements PropertyValue, Serializable {


    private static final Logger logger = LoggerFactory.getLogger(Field.class);

    private final String name;
    private final FieldType type;
    private final Object rawValue;

    public Field() {
        this("", FieldType.STRING, null);
    }

    public Field(String name, FieldType type, Object rawValue) {
        this.name = name;
        this.type = type;
        this.rawValue = rawValue;
    }

    public Boolean isReserved() {
        return FieldDictionary.contains(getName());
    }

    @Override
    public String toString() {

        if (rawValue != null)
            return "Field{" +
                    "name='" + name + '\'' +
                    ", type=" + type +
                    ", rawValue=" + rawValue +
                    '}';
        else
            return "Field{" +
                    "name='" + name + '\'' +
                    ", type=" + type +
                    ", rawValue=null" +
                    '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Field field = (Field) o;

        if (!type.equals(field.type)) return false;
        if (!name.equals(field.name)) return false;
        return Objects.equals(rawValue, field.rawValue);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        if ( rawValue != null ) {
            result = 31 * result + rawValue.hashCode();
        }
        return result;
    }


    public FieldType getType() {
        return type;
    }


    public String getName() {
        return name;
    }


    @Override
    public Object getRawValue() {
        return rawValue;
    }

    @Override
    public String asString() {
        return (rawValue == null) ? null : rawValue.toString();
    }

    @Override
    public Integer asInteger() {
        if (rawValue == null) {
            return null;
        } else if (rawValue instanceof Number) {
            return ((Number) rawValue).intValue();
        } else {
            try {
                return Integer.parseInt(rawValue.toString());
            } catch (Exception ex) {
                logger.error(ex.toString() + " : unable to convert " + rawValue.toString() + " as a int, returning 0");
                return 0;
            }
        }
    }

    @Override
    public Record asRecord() {
        if (rawValue == null) {
            return null;
        } else if (rawValue instanceof Record) {
            return ((Record) rawValue);
        } else return null;
    }

    @Override
    public Long asLong() {
        if (rawValue == null) {
            return null;
        } else {
            if (rawValue instanceof Number) {
                return ((Number) rawValue).longValue();
            } else {
                try {
                    return Long.parseLong(rawValue.toString());
                } catch (Exception ex) {
                    logger.error(ex.toString() + " : unable to convert " + rawValue.toString() + " as a long, returning 0");
                    return 0L;
                }
            }
        }

    }

    @Override
    public Boolean asBoolean() {
        if(rawValue == null)
            return null;

        return BooleanUtils.toBoolean(rawValue.toString());

    }

    @Override
    public Float asFloat() {
        if (rawValue == null) {
            return null;
        } else if (rawValue instanceof Number) {
            return ((Number) rawValue).floatValue();
        } else {
            try {
                return Float.parseFloat(rawValue.toString());
            } catch (Exception ex) {
                try {
                    return Float.parseFloat(rawValue.toString().replaceAll(",", "."));
                } catch (Exception ex2) {
                    logger.error(ex2.toString() + " : unable to convert " + rawValue.toString() + " as a float, returning 0");
                    return 0.0f;
                }
            }
        }
    }

    @Override
    public Double asDouble() {
        if (rawValue == null) {
            return null;
        } else if (rawValue instanceof Number) {
            return ((Number) rawValue).doubleValue();
        } else {
            try {
                return Double.parseDouble(rawValue.toString());
            } catch (Exception ex) {

                try{
                    return Double.parseDouble(rawValue.toString().replaceAll(",", "."));
                }catch (Exception ex2) {
                    logger.error(ex2.toString() + " : unable to convert " + rawValue.toString() + " as a double, returning 0");
                    return 0.0;
                }
            }
        }
    }

    @Override
    public Long asTimePeriod(final TimeUnit timeUnit) {
        return (rawValue == null) ? null : FormatUtils.getTimeDuration(rawValue.toString().trim(), timeUnit);
    }

    @Override
    public boolean isSet() {
        return rawValue != null;
    }

    @Override
    public ControllerService asControllerService() {
        try {
            return (ControllerService) rawValue;
        } catch (Exception ex) {
            throw new IllegalArgumentException("unable to convert field" + rawValue.toString() + " as a ControllerService");
        }
    }


    @Override
    public PropertyValue evaluate(Record record) {
        throw new UnsupportedOperationException("The evaluate(record) method is not available for this type of PropertyValue");
    }
}
