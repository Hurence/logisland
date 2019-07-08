/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.record;

import com.hurence.logisland.component.AbstractPropertyValue;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.util.FormatUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;
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
public class Field extends AbstractPropertyValue implements PropertyValue, Serializable, Comparable<Field> {


    private static final Logger logger = LoggerFactory.getLogger(Field.class);

    private final String name;
    private final FieldType type;

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
        return Objects.deepEquals(rawValue, field.rawValue);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        if (rawValue != null) {
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

    @Override
    public int compareTo(Field o) {
        if (this == o) return 0;
        if (o == null) return 1;
        switch (getType()) {
            case STRING:
                return asString().compareTo(o.asString());
            case INT:
                return asInteger().compareTo(o.asInteger());
            case LONG:
                return asLong().compareTo(o.asLong());
            case FLOAT:
                return asFloat().compareTo(o.asFloat());
            case DOUBLE:
                return asDouble().compareTo(o.asDouble());
            case BOOLEAN:
                return asBoolean().compareTo(o.asBoolean());
            case DATETIME:
                logger.warn("date not yet handled ! Ignored");
                return 0;
            case NULL:
            case ARRAY:
            case BYTES:
            case RECORD:
            case MAP:
            case ENUM:
            case UNION:
                return 0;
            default:
                logger.warn("unknown field type ! '{}'", getType());
                return 0;
        }
    }
}
