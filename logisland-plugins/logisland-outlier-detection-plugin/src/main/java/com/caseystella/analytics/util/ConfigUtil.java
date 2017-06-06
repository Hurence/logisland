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
package com.caseystella.analytics.util;

public enum ConfigUtil {
    INSTANCE;
    public Integer coerceInteger(String field, Object o) {
        return coerceInteger(field, o, false);
    }
    public Integer coerceInteger(String field, Object o, boolean required) {
        if(o instanceof String) {
            return Integer.parseInt(o.toString());
        }
        else if(o instanceof Number) {
            return ((Number)o).intValue();
        }
        throw new RuntimeException(field + ": Unable to coerce " + o + " to a number");
    }

    public Long coerceLong(String field, Object o) {
        return coerceLong(field, o, false);
    }
    public Long coerceLong(String field, Object o, boolean required) {
        if(o instanceof String) {
            return Long.parseLong(o.toString());
        }
        else if(o instanceof Number) {
            return ((Number)o).longValue();
        }
        throw new RuntimeException(field + ": Unable to coerce " + o + " to a number");
    }

    public Double coerceDouble(String field, Object o) {
        return coerceDouble(field, o, false);
    }

    public Double coerceDouble(String field, Object o, boolean required) {
        if(o instanceof String) {
            return Double.parseDouble(o.toString());
        }
        else if(o instanceof Number) {
            return ((Number)o).doubleValue();
        }
        throw new RuntimeException(field + ": Unable to coerce " + o + " to a number");
    }

    public Boolean coerceBoolean(String field, Object o) {
        return coerceBoolean(field, o, false);
    }

    public Boolean coerceBoolean(String field, Object o, boolean required) {
        if(o instanceof String) {
            return o.toString().equalsIgnoreCase("true");
        }
        else if(o instanceof Number) {
            return Math.abs(((Number)o).doubleValue()) > 0;
        }
        else if(o instanceof Boolean) {
            return (Boolean)o;
        }
        throw new RuntimeException(field + ": Unable to coerce " + o + " to a boolean");
    }
}
