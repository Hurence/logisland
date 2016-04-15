package com.caseystella.analytics.converters;

import java.util.HashMap;
import java.util.Map;

public class NOOP implements MappingConverter{
    @Override
    public Map<String, Object> convert(byte[] in, Map<String, Object> config) {
        return new HashMap<String, Object>();
    }
}
