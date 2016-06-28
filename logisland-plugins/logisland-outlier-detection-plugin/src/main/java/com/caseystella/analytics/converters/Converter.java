package com.caseystella.analytics.converters;

import java.io.Serializable;
import java.util.Map;

public interface Converter<T, S> extends Serializable {
    T convert(S in, Map<String, Object> config );
}
