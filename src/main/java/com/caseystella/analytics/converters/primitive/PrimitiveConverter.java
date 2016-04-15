package com.caseystella.analytics.converters.primitive;

import com.caseystella.analytics.converters.MappingConverter;
import com.caseystella.analytics.converters.TimestampConverter;
import com.caseystella.analytics.converters.MeasurementConverter;
import com.google.common.base.Function;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public class PrimitiveConverter {
    public static final String TYPE_CONF = "type";
    public static final String NAME_CONF = "name";
    public enum Type implements Function<byte[], Object> {
        DOUBLE(new Function<byte[], Object>() {
            @Override
            public Object apply(byte[] bytes) {
                return Bytes.toDouble(bytes);
            }
        })
        ,LONG(new Function<byte[], Object>() {
            @Override
            public Object apply(byte[] bytes) {
                return Bytes.toLong(bytes);
            }
        })
        ,INTEGER(new Function<byte[], Object>() {
            @Override
            public Object apply(byte[] bytes) {
                return Bytes.toInt(bytes);
            }
        })
        ,STRING(new Function<byte[], Object>() {
            @Override
            public Object apply(byte[] bytes) {
                return Bytes.toString(bytes);
            }
        })
        ;
        private Function<byte[], Object> _func;
        Type(Function<byte[], Object> func) {
            _func = func;
        }
        @Override
        public Object apply(byte[] bytes) {
            return _func.apply(bytes);
        }
    }

    public static class PrimitiveMeasurementConverter implements MeasurementConverter {

        @Override
        public Double convert(Object in, Map<String, Object> config) {
            if(in instanceof Double) {
                return (Double)in;
            }
            else if(in instanceof Number) {
                return ((Number)in).doubleValue();
            }
            else if(in instanceof String) {
                return Double.parseDouble(in.toString());
            }
            else
            {
                throw new RuntimeException("Unable to convert " + in + " to a double");
            }
        }
    }

    public static class PrimitiveTimestampConverter implements TimestampConverter{
        @Override
        public Long convert(Object in, Map<String, Object> config) {
            if(in instanceof Long) {
                return (Long)in;
            }
            else if(in instanceof Number) {
                return ((Number)in).longValue();
            }
            else if(in instanceof String) {
                return Long.parseLong(in.toString());
            }
            else
            {
                throw new RuntimeException("Unable to convert " + in + " to a long");
            }
        }
    }

    public static class PrimitiveMappingConverter implements MappingConverter {

        @Override
        public Map<String, Object> convert(final byte[] in, final Map<String, Object> config) {
            final Type t = Type.valueOf((String) config.get(TYPE_CONF));
            return new HashMap<String, Object>() {{
                put((String) config.get(NAME_CONF), t.apply(in));
            }};
        }
    }
}
