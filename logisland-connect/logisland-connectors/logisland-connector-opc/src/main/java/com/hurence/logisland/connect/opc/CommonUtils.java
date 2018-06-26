/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.connect.opc;

import com.hurence.opc.OpcData;
import com.hurence.opc.OperationStatus;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.SchemaBuilderException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommonUtils {


    private static final Pattern TAG_FORMAT_MATCHER = Pattern.compile("^([^:]+)(:(\\d+))?$");


    public static boolean validateTagFormat(String tag) {
        return TAG_FORMAT_MATCHER.matcher(tag).matches();
    }


    public static Map.Entry<String, Long> parseTag(String t, Long defaultRefreshPeriod) {
        Matcher matcher = TAG_FORMAT_MATCHER.matcher(t);
        if (matcher.matches()) {
            String tagName = matcher.group(1);
            String refresh = matcher.groupCount() == 3 ? matcher.group(3) : null;
            return new AbstractMap.SimpleEntry<>(tagName, refresh != null ? Long.parseLong(refresh) : defaultRefreshPeriod);
        }
        throw new IllegalArgumentException("" + t + " does not match");
    }

    public static SchemaAndValue convertToNativeType(final Object value) {

        Class<?> cls = value != null ? value.getClass() : Void.class;
        final ArrayList<Object> objs = new ArrayList<>();

        if (cls.isArray()) {
            final Object[] array = (Object[]) value;

            Schema arraySchema = null;

            for (final Object element : array) {
                SchemaAndValue tmp = convertToNativeType(element);
                if (arraySchema == null) {
                    arraySchema = tmp.schema();
                }
                objs.add(tmp.value());
            }

            return new SchemaAndValue(SchemaBuilder.array(arraySchema), objs);
        }

        if (cls.isAssignableFrom(Void.class)) {
            return SchemaAndValue.NULL;
        } else if (cls.isAssignableFrom(String.class)) {
            return new SchemaAndValue(SchemaBuilder.string().optional(), value);
        } else if (cls.isAssignableFrom(Short.class)) {
            return new SchemaAndValue(SchemaBuilder.int16().optional(), value);
        } else if (cls.isAssignableFrom(Integer.class)) {

            return new SchemaAndValue(SchemaBuilder.int32().optional(), value);
        } else if (cls.isAssignableFrom(Long.class)) {

            return new SchemaAndValue(SchemaBuilder.int64().optional(), value);
        } else if (cls.isAssignableFrom(Byte.class)) {
            return new SchemaAndValue(SchemaBuilder.int8().optional(), value);
        } else if (cls.isAssignableFrom(Character.class)) {
            return new SchemaAndValue(SchemaBuilder.int32().optional(), value == null ? null : new Integer(((char) value)));
        } else if (cls.isAssignableFrom(Boolean.class)) {
            return new SchemaAndValue(SchemaBuilder.bool().optional(), value);
        } else if (cls.isAssignableFrom(Float.class)) {
            return new SchemaAndValue(SchemaBuilder.float32().optional(), value);
        } else if (cls.isAssignableFrom(BigDecimal.class)) {
            return new SchemaAndValue(SchemaBuilder.float64().optional(), value == null ? null : ((BigDecimal) value).doubleValue());
        } else if (cls.isAssignableFrom(Double.class)) {
            return new SchemaAndValue(SchemaBuilder.float64().optional(), value);
        } else if (cls.isAssignableFrom(Instant.class)) {
            return new SchemaAndValue(SchemaBuilder.int64().optional(), value == null ? null : ((Instant) value).toEpochMilli());

        }
        throw new SchemaBuilderException("Unknown type presented (" + cls + ")");

    }

    public static Schema buildSchema(Schema valueSchema) {
        SchemaBuilder ret = SchemaBuilder.struct()
                .field(OpcRecordFields.TAG_NAME, SchemaBuilder.string())
                .field(OpcRecordFields.TAG_ID, SchemaBuilder.string())
                .field(OpcRecordFields.TIMESTAMP, SchemaBuilder.int64())
                .field(OpcRecordFields.QUALITY, SchemaBuilder.string())
                .field(OpcRecordFields.UPDATE_PERIOD, SchemaBuilder.int64().optional())
                .field(OpcRecordFields.TAG_GROUP, SchemaBuilder.string().optional())
                .field(OpcRecordFields.OPC_SERVER_DOMAIN, SchemaBuilder.string().optional())
                .field(OpcRecordFields.OPC_SERVER_HOST, SchemaBuilder.string())
                .field(OpcRecordFields.ERROR_CODE, SchemaBuilder.int64().optional())
                .field(OpcRecordFields.ERROR_REASON, SchemaBuilder.string().optional());
        if (valueSchema != null) {
            ret = ret.field(OpcRecordFields.VALUE, valueSchema);
        }
        return ret;
    }

    public static Struct mapToConnectObject(OpcData opcData, TagInfo meta, Schema schema, SchemaAndValue valueSchema, Map<String, Object> additionalProps) {
        Struct value = new Struct(schema)
                .put(OpcRecordFields.TIMESTAMP, opcData.getTimestamp().toEpochMilli())
                .put(OpcRecordFields.TAG_ID, opcData.getTag())
                .put(OpcRecordFields.TAG_NAME, meta.getTagInfo().getName())
                .put(OpcRecordFields.QUALITY, opcData.getQuality().name())
                .put(OpcRecordFields.UPDATE_PERIOD, meta.getRefreshPeriodMillis())
                .put(OpcRecordFields.TAG_GROUP, meta.getTagInfo().getGroup());
        additionalProps.forEach(value::put);


        if (valueSchema.value() != null) {
            value = value.put(OpcRecordFields.VALUE, valueSchema.value());
        }
        if (opcData.getOperationStatus().getLevel().compareTo(OperationStatus.Level.INFO) > 0) {
            value.put(OpcRecordFields.ERROR_CODE, opcData.getOperationStatus().getCode());
            if (opcData.getOperationStatus().getMessageDetail().isPresent()) {
                value.put(OpcRecordFields.ERROR_REASON,
                        opcData.getOperationStatus().getMessageDetail().get());
            }
        }
        return value;
    }
}
