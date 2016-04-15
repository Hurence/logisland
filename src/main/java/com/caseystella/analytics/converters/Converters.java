package com.caseystella.analytics.converters;

import com.caseystella.analytics.converters.csv.CSVConverter;
import com.caseystella.analytics.converters.primitive.DateConverter;
import com.caseystella.analytics.converters.primitive.PrimitiveConverter;

import java.util.HashMap;
import java.util.Map;

public class Converters {

    private static Map<String, Class<? extends MappingConverter>> _mappingConverters = new HashMap<String, Class<? extends MappingConverter>>() {{
        put(CSVConverter.class.getSimpleName(), CSVConverter.class);
        put(DateConverter.class.getSimpleName(), DateConverter.DateMappingConverter.class);
        put(PrimitiveConverter.class.getSimpleName(), PrimitiveConverter.PrimitiveMappingConverter.class);
        put(NOOP.class.getSimpleName(), NOOP.class);
    }};

    private static Map<String, Class<? extends TimestampConverter>> _timestampConverters = new HashMap<String, Class<? extends TimestampConverter>>() {{
        put(DateConverter.class.getSimpleName(), DateConverter.DateTimestampConverter.class);
        put(PrimitiveConverter.class.getSimpleName(), PrimitiveConverter.PrimitiveTimestampConverter.class);
    }};

    private static Map<String, Class<? extends MeasurementConverter>> _measurementConverters = new HashMap<String, Class<? extends MeasurementConverter>>() {{
        put(PrimitiveConverter.class.getSimpleName(), PrimitiveConverter.PrimitiveMeasurementConverter.class);
    }};
    public static MappingConverter getMappingConverter(String converter) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getConverter(converter, _mappingConverters);
    }
    public static TimestampConverter getTimestampConverter(String converter) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getConverter(converter, _timestampConverters);
    }
    public static MeasurementConverter getMeasurementConverter(String converter) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getConverter(converter, _measurementConverters);
    }
    private static <T> T getConverter(String converter, Map<String, Class<? extends T>> converters) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<? extends T > clazz = converters.get(converter);
        if(clazz == null) {
            clazz = (Class<? extends T>) Converters.class.forName(converter);
        }
        return clazz.newInstance();
    }
}
