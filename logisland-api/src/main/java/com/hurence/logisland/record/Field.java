/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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

import com.hurence.logisland.component.PropertyValue;

import java.io.Serializable;

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
        return rawValue.equals(field.rawValue);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + rawValue.hashCode();
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
            return null;
        }
    }

    @Override
    public Long asLong() {
        if (rawValue == null) {
            return null;
        } else if (rawValue instanceof Number) {
            return ((Number) rawValue).longValue();
        } else {
            return null;
        }
    }

    @Override
    public Boolean asBoolean() {
        return (rawValue == null) ? null : (boolean) rawValue;
    }

    @Override
    public Float asFloat() {
        if (rawValue == null) {
            return null;
        } else if (rawValue instanceof Number) {
            return ((Number) rawValue).floatValue();
        } else {
            return null;
        }
    }

    @Override
    public Double asDouble() {
        if (rawValue == null) {
            return null;
        } else if (rawValue instanceof Number) {
            return ((Number) rawValue).doubleValue();
        } else {
            return null;
        }
    }

    @Override
    public boolean isSet() {
        return rawValue != null;
    }

}
