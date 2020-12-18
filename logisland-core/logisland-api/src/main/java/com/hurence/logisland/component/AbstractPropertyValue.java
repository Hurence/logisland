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
package com.hurence.logisland.component;

import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.util.FormatUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by mathieu on 08/06/17.
 */
public abstract class AbstractPropertyValue implements PropertyValue {

    protected Object rawValue;
    protected ControllerServiceLookup serviceLookup;
    protected VariableRegistry variableRegistry;

    private static final Logger logger = LoggerFactory.getLogger(AbstractPropertyValue.class);

    @Override
    public Object getRawValue() {
        return rawValue;
    }

    @Override
    public String asString() {
        if (getRawValue() == null) {
            return null;
        } else if (getRawValue() instanceof char[]) {
            return new String(asChars());
        } else if (getRawValue() instanceof Field) {
            return (String) ((Field)getRawValue()).getRawValue();
        }
        return getRawValue().toString();
    }

    @Override
    public Integer asInteger() {
        if (getRawValue() == null) {
            return null;
        } else if (getRawValue() instanceof Number) {
            return ((Number) rawValue).intValue();
        } else {
            try {
                return Integer.parseInt(asString().trim());
            } catch (NumberFormatException ex) {
                logger.error(" : unable to convert " + rawValue.toString() + " as a int", ex);
                throw ex;
            }
        }
    }

    @Override
    public Long asLong() {
        if (getRawValue() == null) {
            return null;
        } else {
            if (getRawValue() instanceof Number) {
                return ((Number) getRawValue()).longValue();
            } else if (getRawValue() instanceof Date) {
                return ((Date) getRawValue()).getTime();
            } else {
                try {
                    return Long.parseLong(asString().trim());
                } catch (Exception ex) {
                    logger.error(" : unable to convert " + rawValue.toString() + " as a long", ex);
                    throw ex;
                }
            }
        }
    }


    @Override
    public Boolean asBoolean() {
        return (getRawValue() == null) ? null : Boolean.parseBoolean(asString().trim());
    }

    @Override
    public Float asFloat() {
        if (getRawValue() == null) {
            return null;
        } else if (getRawValue() instanceof Number) {
            return ((Number) getRawValue()).floatValue();
        } else {
            try {
                return Float.parseFloat(asString().trim());
            } catch (Exception ex) {
                try {
                    return Float.parseFloat(getRawValue().toString().replaceAll(",", "."));
                } catch (Exception ex2) {
                    logger.error(" : unable to convert " + rawValue.toString() + " as a float", ex2);
                    throw ex2;
                }
            }
        }
    }

    @Override
    public Double asDouble() {
        if (getRawValue() == null) {
            return null;
        } else if (getRawValue() instanceof Number) {
            return ((Number) getRawValue()).doubleValue();
        } else {
            try {
                return Double.parseDouble(asString().trim());
            } catch (Exception ex) {
                try {
                    return Double.parseDouble(asString().trim().replaceAll(",", "."));
                } catch (Exception ex2) {
                    logger.error(" : unable to convert " + rawValue.toString() + " as a double", ex2);
                    throw ex2;
                }
            }
        }
    }

    @Override
    public Long asTimePeriod(final TimeUnit timeUnit) {
        return (rawValue == null) ? null : FormatUtils.getTimeDuration(asString().trim(), timeUnit);
    }

    @Override
    public byte[] asBytes() {
        if (getRawValue() == null) {
            return null;
        } else if (getRawValue() instanceof byte[]) {
            return (byte[]) getRawValue();
        } else if (getRawValue() instanceof String) {
            return asString().getBytes();
        } else {
            logger.error(" : unable to convert " + rawValue.toString() + " as a byte[]");
            throw new IllegalArgumentException("not an array of bytes");
        }
    }

    @Override
    public char[] asChars() {
        if (getRawValue() == null) {
            return null;
        } else if (getRawValue() instanceof char[]) {
            return (char[]) getRawValue();
        } else {
            logger.error(" : unable to convert " + rawValue.toString() + " as a char[]");
            throw new IllegalArgumentException("not an array of chars");
        }
    }

    @Override
    public char asChar() {
        if (getRawValue() == null) {
            throw new IllegalArgumentException("null is not a char");
        } else if (getRawValue() instanceof Character) {
            return ((char) getRawValue());
        } else {
            try {
                return asString().charAt(0);
            } catch (Exception ex) {
                logger.error(" : unable to convert " + rawValue.toString() + " as a char", ex);
                throw ex;
            }
        }
    }

    @Override
    public boolean isSet() {
        return getRawValue() != null;
    }

    //    @Override
//    public Record asRecord() {
//        return (getRawValue() == null) ? null : new StandardRecord()
//                .setStringField(FieldDictionary.RECORD_VALUE, asString().trim());
//    }
//TODO verify if ok ? otherwise override in child Field
    @Override
    public Record asRecord() {
        if (getRawValue() == null) {
            return null;
        } else if (getRawValue() instanceof Record) {
            return ((Record) rawValue);
        } else {
            //logger.error(" : unable to convert " + rawValue.toString() + " as a Record[]");
            return null;
        }
    }

    @Override
    public ControllerService asControllerService() {
        if (getRawValue() == null || getRawValue().equals("") || serviceLookup == null) {
            return null;
        }

        return serviceLookup.getControllerService(asString());
    }


    @Override
    public PropertyValue evaluate(Record record) {
        // does nothing
        return this;
    }


}
