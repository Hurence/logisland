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

package com.hurence.logisland.connect.opcda;

import com.hurence.opc.OpcData;
import com.hurence.opc.OpcOperations;
import com.hurence.opc.da.OpcDaConnectionProfile;
import com.hurence.opc.da.OpcDaOperations;
import com.hurence.opc.util.AutoReconnectOpcOperations;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.SchemaBuilderException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.jinterop.dcom.common.JIException;
import org.jinterop.dcom.core.JIVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * OPC-DA Worker task.
 */
public class OpcDaSourceTask extends SourceTask {

    private static class TagInfo {
        final String group;
        final String name;

        public TagInfo(String tag) {
            int idx = tag.lastIndexOf('.');
            if (idx > 0) {
                this.group = tag.substring(0, idx);
            } else {
                this.group = "";
            }
            this.name = tag.substring(idx + 1);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(OpcDaSourceTask.class);

    private OpcOperations<OpcDaConnectionProfile, OpcData> opcOperations;
    private TransferQueue<SourceRecord> transferQueue;
    private long sleepTime = 1000;
    private String tags[];
    private Map<String, TagInfo> tagInfoMap;
    private ExecutorService executorService;
    private String host;
    private String domain;
    private volatile boolean running = false;

    private OpcDaConnectionProfile propertiesToConnectionProfile(Map<String, String> properties) {
        OpcDaConnectionProfile ret = new OpcDaConnectionProfile();
        ret.setHost(properties.get(OpcDaSourceConnector.PROPERTY_HOST));
        ret.setComClsId(properties.get(OpcDaSourceConnector.PROPERTY_CLSID));
        ret.setComProgId(properties.get(OpcDaSourceConnector.PROPERTY_PROGID));
        if (properties.containsKey(OpcDaSourceConnector.PROPERTY_PORT)) {
            ret.setPort(Integer.parseInt(properties.get(OpcDaSourceConnector.PROPERTY_PORT)));
        }
        ret.setUser(properties.get(OpcDaSourceConnector.PROPERTY_USER));
        ret.setPassword(properties.get(OpcDaSourceConnector.PROPERTY_PASSWORD));
        ret.setDomain(properties.get(OpcDaSourceConnector.PROPERTY_DOMAIN));
        if (properties.containsKey(OpcDaSourceConnector.PROPERTY_DIRECT_READ)) {
            ret.setDirectRead(Boolean.parseBoolean(properties.get(OpcDaSourceConnector.PROPERTY_DIRECT_READ)));
        }
        if (properties.containsKey(OpcDaSourceConnector.PROPERTY_REFRESH_PERIOD)) {
            ret.setRefreshPeriodMillis(Long.parseLong(properties.get(OpcDaSourceConnector.PROPERTY_REFRESH_PERIOD)));
        }
        if (properties.containsKey(OpcDaSourceConnector.PROPERTY_SOCKET_TIMEOUT)) {
            ret.setSocketTimeout(Duration.ofMillis(Long.parseLong(properties.get(OpcDaSourceConnector.PROPERTY_SOCKET_TIMEOUT))));
        }
        return ret;
    }

    private SchemaAndValue convertToNativeType(final JIVariant type) {
        try {
            if (type.isArray()) {
                final ArrayList<Object> objs = new ArrayList<>();
                final Object[] array = (Object[]) type.getObjectAsArray().getArrayInstance();

                Schema arraySchema = null;

                for (final Object element : array) {
                    SchemaAndValue tmp = convertToNativeType((JIVariant) element);
                    if (arraySchema == null) {
                        arraySchema = tmp.schema();
                    }
                    objs.add(tmp.value());
                }

                return new SchemaAndValue(SchemaBuilder.array(arraySchema), objs.toArray());
            }

            switch (type.getType()) {
                case JIVariant.VT_NULL:
                case JIVariant.VT_ERROR:
                    return SchemaAndValue.NULL;
                case JIVariant.VT_BSTR:
                    return new SchemaAndValue(SchemaBuilder.string().optional(), type.getObjectAsString().getString());
                case JIVariant.VT_I2:
                case JIVariant.VT_UI2:
                    return new SchemaAndValue(SchemaBuilder.int16().optional(), type.getObjectAsShort());
                case JIVariant.VT_I4:
                case JIVariant.VT_UI4:
                    return new SchemaAndValue(SchemaBuilder.int32().optional(), type.getObjectAsInt());
                case JIVariant.VT_I8:
                case JIVariant.VT_UINT:
                case JIVariant.VT_INT:
                    return new SchemaAndValue(SchemaBuilder.int64().optional(), type.getObjectAsLong());
                case JIVariant.VT_UI1:
                case JIVariant.VT_I1:
                    return new SchemaAndValue(SchemaBuilder.int8().optional(), type.getObjectAsUnsigned().getValue());
                case JIVariant.VT_BOOL:
                    return new SchemaAndValue(SchemaBuilder.bool().optional(), type.getObjectAsBoolean());
                case JIVariant.VT_R4:
                    return new SchemaAndValue(SchemaBuilder.float32().optional(), type.getObjectAsFloat());
                case JIVariant.VT_DECIMAL:
                case JIVariant.VT_R8:
                    return new SchemaAndValue(SchemaBuilder.float64().optional(), type.getObjectAsDouble());
                case JIVariant.VT_DATE:
                    return new SchemaAndValue(SchemaBuilder.int64().optional(), type.getObjectAsDate().toInstant().toEpochMilli());
                default:
                    throw new SchemaBuilderException("Unknown type presented (" + type.getType() + ")");
            }
        } catch (final JIException e) {
            throw new SchemaBuilderException("Failed to convert variant type to native object: " + e.getMessage(), e);
        }
    }

    private Schema buildSchema(Schema valueSchema) {
        SchemaBuilder ret = SchemaBuilder.struct()
                .field(OpcDaFields.TAG_NAME, SchemaBuilder.string())
                .field(OpcDaFields.TIMESTAMP, SchemaBuilder.int64())
                .field(OpcDaFields.QUALITY, SchemaBuilder.int32().optional())
                .field(OpcDaFields.UPDATE_PERIOD, SchemaBuilder.int64().optional())
                .field(OpcDaFields.TAG_GROUP, SchemaBuilder.string().optional())
                .field(OpcDaFields.OPC_SERVER_DOMAIN, SchemaBuilder.string().optional())
                .field(OpcDaFields.OPC_SERVER_HOST, SchemaBuilder.string());

        if (valueSchema != null) {
            ret = ret.field(OpcDaFields.VALUE, valueSchema);
        } else {
            ret = ret.field(OpcDaFields.ERROR_CODE, SchemaBuilder.int32().optional());
        }
        return ret;
    }


    @Override
    public void start(Map<String, String> props) {
        transferQueue = new LinkedTransferQueue<>();
        opcOperations = new AutoReconnectOpcOperations<>(new OpcDaOperations());
        OpcDaConnectionProfile connectionProfile = propertiesToConnectionProfile(props);
        tags = props.get(OpcDaSourceConnector.PROPERTY_TAGS).split(",");
        host = connectionProfile.getHost();
        domain = connectionProfile.getDomain() != null ? connectionProfile.getDomain() : "";
        tagInfoMap = Arrays.stream(tags).collect(Collectors.toMap(Function.identity(), TagInfo::new));
        sleepTime = connectionProfile.getRefreshPeriodMillis();
        opcOperations.connect(connectionProfile);
        if (!opcOperations.awaitConnected()) {
            throw new ConnectException("Unable to connect");
        }
        logger.info("Started OPC-DA task for tags {}", tags);

        running = true;
        executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            while (running) {
                try {
                    opcOperations.stream(tags)
                            .map(opcData -> {
                                SchemaAndValue tmp = convertToNativeType(opcData.getValue());
                                Schema valueSchema = buildSchema(tmp.schema());
                                TagInfo meta = tagInfoMap.get(opcData.getTag());
                                Struct value = new Struct(valueSchema)
                                        .put(OpcDaFields.TIMESTAMP, opcData.getTimestamp().toEpochMilli())
                                        .put(OpcDaFields.TAG_NAME, opcData.getTag())
                                        .put(OpcDaFields.QUALITY, opcData.getQuality())
                                        .put(OpcDaFields.UPDATE_PERIOD, sleepTime)
                                        .put(OpcDaFields.TAG_GROUP, meta.group)
                                        .put(OpcDaFields.OPC_SERVER_HOST, host)
                                        .put(OpcDaFields.OPC_SERVER_DOMAIN, domain);

                                if (tmp.value() != null) {
                                    value = value.put(OpcDaFields.VALUE, tmp.value());
                                }
                                try {
                                    if (opcData.getValue().getType() == JIVariant.VT_ERROR) {
                                        value.put(OpcDaFields.ERROR_CODE, opcData.getValue().getObjectAsSCODE());
                                    }
                                } catch (JIException e) {
                                    logger.warn("Unable to extract error code from tag value");
                                }
                                return new SourceRecord(
                                        Collections.singletonMap(OpcDaFields.TAG_NAME, opcData.getTag()),
                                        Collections.singletonMap(OpcDaFields.TIMESTAMP, opcData.getTimestamp().toEpochMilli()),
                                        "",
                                        SchemaBuilder.STRING_SCHEMA,
                                        "/" + domain + "/" + host + "/" + opcData.getTag(),
                                        valueSchema,
                                        value);
                            })
                            .forEach(sourceRecord -> {
                                try {
                                    transferQueue.put(sourceRecord);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException("Interrupted", e);
                                }
                            });
                } catch (Throwable t) {
                    logger.warn("OPC-DA streaming interrupted", t);
                }
            }
        });
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (transferQueue.size() == 0) {
            Thread.sleep(sleepTime);
        }
        List<SourceRecord> ret = new ArrayList<>();
        transferQueue.drainTo(ret);
        return ret;
    }

    @Override
    public void stop() {
        running = false;
        if (opcOperations != null) {
            executorService.shutdown();
            executorService = null;
            opcOperations.disconnect();
            opcOperations.awaitDisconnected();
            transferQueue = null;
        }
        logger.info("Stopped OPC-DA task for tags {}", tags);
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }
}
