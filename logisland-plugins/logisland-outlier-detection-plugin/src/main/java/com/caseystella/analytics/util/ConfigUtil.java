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
