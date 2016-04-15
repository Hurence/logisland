package com.caseystella.analytics.converters.primitive;

import com.caseystella.analytics.converters.Converter;
import com.caseystella.analytics.converters.MappingConverter;
import com.caseystella.analytics.converters.TimestampConverter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DateConverter {
    public static final String FORMAT_CONF = "format";
    public static final String NAME_CONF = "name";
    public static final String TO_TS_CONF = "to_ts";

    public static class DateTimestampConverter implements TimestampConverter {
        @Override
        public Long convert(Object in, Map<String, Object> config) {
            if(in instanceof Date) {
                return ((Date)in).getTime();
            }
            else if(in instanceof String)
            {
                String format = (String) config.get(FORMAT_CONF);
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                String s = in.toString();
                try {
                    Date d = sdf.parse(s);
                    return d.getTime();
                } catch (ParseException e) {
                    throw new RuntimeException("Malformed Date: " + s);
                }
            }
            else {
                throw new RuntimeException("Unable to convert " + in + " to date");
            }
        }
    }

    public static class DateMappingConverter implements MappingConverter{
        @Override
        public Map<String, Object> convert(byte[] in, Map<String, Object> config) {
            String format = (String) config.get(FORMAT_CONF);
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            String s = (String) PrimitiveConverter.Type.STRING.apply(in);
            Map<String, Object> ret = new HashMap<>();
            try {
                Date d = sdf.parse(s);
                if (config.containsKey(TO_TS_CONF)) {
                    ret.put((String) config.get(NAME_CONF), d.getTime());
                } else {
                    ret.put((String) config.get(NAME_CONF), d);
                }
            } catch (ParseException e) {
                throw new RuntimeException("Malformed Date: " + s);
            }
            return ret;
        }
    }
}
