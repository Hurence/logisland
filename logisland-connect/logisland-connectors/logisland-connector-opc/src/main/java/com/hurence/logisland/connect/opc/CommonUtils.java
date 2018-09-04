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
package com.hurence.logisland.connect.opc;

import com.hurence.opc.OpcData;
import com.hurence.opc.OperationStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.SchemaBuilderException;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CommonUtils {


    /**
     * Extract structured info from tag properties.
     *
     * @param properties the tag properties given by the OPC connector
     * @return a list of {@link TagInfo}
     */
    public static final List<TagInfo> parseTagsFromProperties(Map<String, String> properties) {

        List<String> tagIds = Arrays.asList(properties.get(CommonDefinitions.PROPERTY_TAGS_ID).split(","));
        List<Duration> tagSamplings = Arrays.stream(properties.get(CommonDefinitions.PROPERTY_TAGS_SAMPLING_RATE).split(","))
                .map(Duration::parse).collect(Collectors.toList());
        List<StreamingMode> tagModes = Arrays.stream(properties.get(CommonDefinitions.PROPERTY_TAGS_STREAM_MODE).split(","))
                .map(StreamingMode::valueOf).collect(Collectors.toList());
        List<TagInfo> ret = new ArrayList<>();
        for (int i = 0; i < tagIds.size(); i++) {
            ret.add(new TagInfo(tagIds.get(i), tagSamplings.get(i), tagModes.get(i)));
        }
        return ret;

    }

    /**
     * Validate the tag configuration.
     *
     * @param tags  the list of tag id.
     * @param freqs the list of tag sampling frequencies.
     * @param modes the list of tag streaming modes.
     * @throws IllegalArgumentException in case something is bad.
     */
    public static final void validateTagConfig(List<String> tags, List<String> freqs, List<String> modes) {
        //validate
        if (tags == null || tags.isEmpty()) {
            throw new IllegalArgumentException("Tag id list should not be empty");
        }
        if (freqs == null || freqs.size() != tags.size()) {
            throw new IllegalArgumentException("You should provide exactly one sampling rate per tag id");
        }
        if (modes == null || modes.size() != tags.size()) {
            throw new IllegalArgumentException("You should provide exactly one streaming mode per tag id");
        }
        freqs.forEach(freq -> {
            try {
                if (StringUtils.isNotBlank(freq)) {
                    Duration.parse(freq);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Unrecognized sampling rate: " + freq);
            }
        });
        modes.forEach(mode -> {
            try {
                if (StringUtils.isNotBlank(mode)) {
                    StreamingMode.valueOf(mode);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Unrecognized streaming mode: " + mode);
            }
        });
    }


    /**
     * Convert a java object to a {@link SchemaAndValue} data.
     *
     * @param value
     * @return
     */
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

    /**
     * Build the connector data schema.
     *
     * @param valueSchema the schema of the tag value read (since we can have variants).
     * @return
     */
    public static Schema buildSchema(Schema valueSchema) {
        SchemaBuilder ret = SchemaBuilder.struct()
                .field(OpcRecordFields.TAG_ID, SchemaBuilder.string())
                .field(OpcRecordFields.SOURCE_TIMESTAMP, SchemaBuilder.int64())
                .field(OpcRecordFields.SAMPLED_TIMESTAMP, SchemaBuilder.int64())
                .field(OpcRecordFields.QUALITY, SchemaBuilder.string())
                .field(OpcRecordFields.SAMPLING_RATE, SchemaBuilder.int64().optional())
                .field(OpcRecordFields.OPC_SERVER, SchemaBuilder.string())
                .field(OpcRecordFields.ERROR_CODE, SchemaBuilder.int64().optional())
                .field(OpcRecordFields.ERROR_REASON, SchemaBuilder.string().optional());
        if (valueSchema != null) {
            ret = ret.field(OpcRecordFields.VALUE, valueSchema);
        }
        return ret;
    }

    /**
     * Maps an opc data to a kafka connect object.
     *
     * @param opcData         the read data
     * @param timestamp       the data timestamp
     * @param meta            the tag information
     * @param schema
     * @param valueSchema
     * @param additionalProps
     * @return
     */
    public static Struct mapToConnectObject(OpcData opcData, Instant timestamp, TagInfo meta, Schema schema,
                                            SchemaAndValue valueSchema, Map<String, Object> additionalProps) {
        Struct value = new Struct(schema)
                .put(OpcRecordFields.SAMPLED_TIMESTAMP, timestamp.toEpochMilli())
                .put(OpcRecordFields.SOURCE_TIMESTAMP, opcData.getTimestamp().toEpochMilli())
                .put(OpcRecordFields.TAG_ID, opcData.getTag())
                .put(OpcRecordFields.QUALITY, opcData.getQuality().name())
                .put(OpcRecordFields.SAMPLING_RATE, meta.getSamplingInterval().toMillis());
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
