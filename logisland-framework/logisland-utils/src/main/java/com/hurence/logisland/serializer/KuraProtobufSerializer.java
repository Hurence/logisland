/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.serializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hurence.logisland.record.*;
import com.hurence.logisland.util.GZipUtil;
import org.apache.commons.io.IOUtils;
import org.eclipse.kura.core.message.protobuf.KuraPayloadProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

public class KuraProtobufSerializer implements RecordSerializer {

    public static final String KURA_RECORD = "kura_record";
    public static final String KURA_METRIC = "kura_metric";

    private static Logger logger = LoggerFactory.getLogger(KuraProtobufSerializer.class);

    @Override
    public void serialize(OutputStream objectDataOutput, Record record) throws RecordSerializationException {

        final KuraPayloadProto.KuraPayload.Builder protoMsg = KuraPayloadProto.KuraPayload.newBuilder();

        protoMsg.setTimestamp(record.getTime().getTime());

        if (record.hasPosition()) {
            protoMsg.setPosition(buildPositionProtoBuf(record.getPosition()));
        }

        record.getAllFields()
                .stream()
                .filter(field -> field.getType().equals(FieldType.RECORD) && field.asRecord().getType().equals(KURA_METRIC))
                .forEach(field -> {

                    final Record value = field.asRecord();
                    try {
                        final KuraPayloadProto.KuraPayload.KuraMetric.Builder metricB = KuraPayloadProto.KuraPayload.KuraMetric
                                .newBuilder();
                        metricB.setName(field.getName());

                        setProtoKuraMetricValue(metricB, field.asRecord().getField(FieldDictionary.RECORD_VALUE).getRawValue());
                        metricB.build();

                        protoMsg.addMetric(metricB);
                    } catch (final Exception eihte) {
                        try {
                            System.err.println("During serialization, ignoring metric named:  " + field.getName()
                                    + "  . Unrecognized value type: " + value.getClass().getName() + ".");
                        } catch (final NullPointerException npe) {
                            System.err.println("During serialization, ignoring metric named: " + field.getName() + ". The value is null.");
                        }
                        throw new RuntimeException(eihte);
                    }
                });


       /* if (record.hasField(FieldDictionary.RECORD_BODY)) {
            protoMsg.setBody(ByteString.copyFrom(record.getField(FieldDictionary.RECORD_BODY).asString().getBytes()));
        }*/
        protoMsg.setBody(ByteString.copyFrom(record.getField(FieldDictionary.RECORD_ID).asString().getBytes()));


        try {
            byte[] paylod = protoMsg.build().toByteArray();
            objectDataOutput.write(paylod);
        } catch (IOException e) {
            logger.debug("serialization failed", e);
        }


    }

    @Override
    public Record deserialize(InputStream objectDataInput) throws RecordSerializationException {


        Record kuraRecord = new StandardRecord(KURA_RECORD);
        try {
            byte[] bytes = IOUtils.toByteArray(objectDataInput);
            if (GZipUtil.isCompressed(bytes)) {
                try {
                    bytes = GZipUtil.decompress(bytes);
                } catch (final IOException e) {
                    logger.debug("Decompression failed", e);
                }
            }

            KuraPayloadProto.KuraPayload protoMsg = null;
            try {
                protoMsg = KuraPayloadProto.KuraPayload.parseFrom(bytes);
            } catch (final InvalidProtocolBufferException ipbe) {
                throw new RuntimeException(ipbe);
            }

            if (protoMsg.hasTimestamp()) {
                kuraRecord.setTime(protoMsg.getTimestamp());
            }

            if (protoMsg.hasPosition()) {
                kuraRecord.setPosition(this.buildFromProtoBuf(protoMsg.getPosition()));
            }

            for (int i = 0; i < protoMsg.getMetricCount(); i++) {
                final String name = protoMsg.getMetric(i).getName();
                try {

                    Record kuraMetric = new StandardRecord(KURA_METRIC);

                    final Object value = this.getProtoKuraMetricValue(protoMsg.getMetric(i),
                            protoMsg.getMetric(i).getType());

                    switch (protoMsg.getMetric(i).getType()) {
                        case DOUBLE:
                            kuraMetric.setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, value);
                            break;
                        case BOOL:
                            kuraMetric.setField(FieldDictionary.RECORD_VALUE, FieldType.BOOLEAN, value);
                            break;
                        case BYTES:
                            kuraMetric.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, value);
                            break;
                        case FLOAT:
                            kuraMetric.setField(FieldDictionary.RECORD_VALUE, FieldType.FLOAT, value);
                            break;
                        case INT32:
                            kuraMetric.setField(FieldDictionary.RECORD_VALUE, FieldType.INT, value);
                            break;
                        case INT64:
                            kuraMetric.setField(FieldDictionary.RECORD_VALUE, FieldType.LONG, value);
                            break;
                        case STRING:
                            kuraMetric.setField(FieldDictionary.RECORD_VALUE, FieldType.STRING, value);
                            break;
                        default:
                            logger.error("unknown metric value type for Kura record");
                            break;
                    }

                    kuraRecord.setField(name, FieldType.RECORD, kuraMetric);

                } catch (final Exception ihte) {
                    logger.debug("During deserialization, ignoring metric named: " + name
                            + ". Unrecognized value type: " + protoMsg.getMetric(i).getType(), ihte);
                }
            }

            if (protoMsg.hasBody()) {
                kuraRecord.setId(protoMsg.getBody().toStringUtf8());

            }
        } catch (IOException ex) {
            logger.error("unknown error for Kura record", ex);
        }


        return kuraRecord;
    }


    private Position buildFromProtoBuf(final KuraPayloadProto.KuraPayload.KuraPosition protoPosition) {
        final Position position = Position.from(
                protoPosition.hasAltitude() ? protoPosition.getAltitude() : Double.MIN_VALUE,
                protoPosition.hasHeading() ? protoPosition.getHeading() : Double.MIN_VALUE,
                protoPosition.hasLatitude() ? protoPosition.getLatitude() : Double.MIN_VALUE,
                protoPosition.hasLongitude() ? protoPosition.getLongitude() : Double.MIN_VALUE,
                protoPosition.hasPrecision() ? protoPosition.getPrecision() : Double.MIN_VALUE,
                protoPosition.hasSatellites() ? protoPosition.getSatellites() : Integer.MIN_VALUE,
                protoPosition.hasStatus() ? protoPosition.getStatus() : Integer.MIN_VALUE,
                protoPosition.hasSpeed() ? protoPosition.getSpeed() : Double.MIN_VALUE,
                protoPosition.hasTimestamp() ? new Date(protoPosition.getTimestamp()) : new Date(0)
        );


        return position;
    }

    private Object getProtoKuraMetricValue(final KuraPayloadProto.KuraPayload.KuraMetric metric,
                                           final KuraPayloadProto.KuraPayload.KuraMetric.ValueType type) throws Exception {
        switch (type) {

            case DOUBLE:
                return metric.getDoubleValue();

            case FLOAT:
                return metric.getFloatValue();

            case INT64:
                return metric.getLongValue();

            case INT32:
                return metric.getIntValue();

            case BOOL:
                return metric.getBoolValue();

            case STRING:
                return metric.getStringValue();

            case BYTES:
                final ByteString bs = metric.getBytesValue();
                return bs.toByteArray();

            default:
                throw new Exception(type.name());
        }
    }


    private static void setProtoKuraMetricValue(final KuraPayloadProto.KuraPayload.KuraMetric.Builder metric,
                                                final Object o) throws Exception {

        if (o instanceof String) {
            metric.setType(KuraPayloadProto.KuraPayload.KuraMetric.ValueType.STRING);
            metric.setStringValue((String) o);
        } else if (o instanceof Double) {
            metric.setType(KuraPayloadProto.KuraPayload.KuraMetric.ValueType.DOUBLE);
            metric.setDoubleValue((Double) o);
        } else if (o instanceof Integer) {
            metric.setType(KuraPayloadProto.KuraPayload.KuraMetric.ValueType.INT32);
            metric.setIntValue((Integer) o);
        } else if (o instanceof Float) {
            metric.setType(KuraPayloadProto.KuraPayload.KuraMetric.ValueType.FLOAT);
            metric.setFloatValue((Float) o);
        } else if (o instanceof Long) {
            metric.setType(KuraPayloadProto.KuraPayload.KuraMetric.ValueType.INT64);
            metric.setLongValue((Long) o);
        } else if (o instanceof Boolean) {
            metric.setType(KuraPayloadProto.KuraPayload.KuraMetric.ValueType.BOOL);
            metric.setBoolValue((Boolean) o);
        } else if (o instanceof byte[]) {
            metric.setType(KuraPayloadProto.KuraPayload.KuraMetric.ValueType.BYTES);
            metric.setBytesValue(ByteString.copyFrom((byte[]) o));
        } else if (o == null) {
            throw new Exception("null value");
        } else {
            throw new Exception(o.getClass().getName());
        }
    }


    private KuraPayloadProto.KuraPayload.KuraPosition buildPositionProtoBuf(Position position) {
        KuraPayloadProto.KuraPayload.KuraPosition.Builder protoPos = null;
        protoPos = KuraPayloadProto.KuraPayload.KuraPosition.newBuilder();

        if (position.getLatitude() != null) {
            protoPos.setLatitude(position.getLatitude());
        }
        if (position.getLongitude() != null) {
            protoPos.setLongitude(position.getLongitude());
        }
        if (position.getAltitude() != null) {
            protoPos.setAltitude(position.getAltitude());
        }
        if (position.getPrecision() != null) {
            protoPos.setPrecision(position.getPrecision());
        }
        if (position.getHeading() != null) {
            protoPos.setHeading(position.getHeading());
        }
        if (position.getSpeed() != null) {
            protoPos.setSpeed(position.getSpeed());
        }
        if (position.getTimestamp() != null) {
            protoPos.setTimestamp(position.getTimestamp().getTime());
        }
        if (position.getSatellites() != null) {
            protoPos.setSatellites(position.getSatellites());
        }
        if (position.getStatus() != null) {
            protoPos.setStatus(position.getStatus());
        }
        return protoPos.build();
    }


}
