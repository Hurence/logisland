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

import com.hurence.opc.da.OpcDaConnectionProfile;
import com.hurence.opc.da.OpcDaOperations;
import com.hurence.opc.da.OpcDaSession;
import com.hurence.opc.da.OpcDaSessionProfile;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.SchemaBuilderException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * OPC-DA Worker task.
 */
public class OpcDaSourceTask extends SourceTask {

    private static class TagInfo {
        final String group;
        final String name;
        final Long refreshPeriodMillis;

        public TagInfo(String raw, long defaultRefreshPeriod) {
            Map.Entry<String, Long> parsed = OpcDaSourceConnector.parseTag(raw, defaultRefreshPeriod);
            String tag = parsed.getKey();
            this.refreshPeriodMillis = parsed.getValue();
            int idx = tag.lastIndexOf('.');
            if (idx > 0) {
                this.group = tag.substring(0, idx);
            } else {
                this.group = "";
            }
            this.name = tag;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(OpcDaSourceTask.class);

    private SmartOpcOperations<OpcDaConnectionProfile, OpcDaSessionProfile, OpcDaSession> opcOperations;
    private TransferQueue<SourceRecord> transferQueue;
    private Lock lock = new ReentrantLock();
    private String tags[];
    private Map<String, OpcDaSession> sessions;
    private Map<String, TagInfo> tagInfoMap;
    private Set<String> tagReadingQueue;
    private ScheduledExecutorService executorService;
    private String host;
    private String domain;
    private boolean directRead;
    private long defaultRefreshPeriodMillis;
    private long minWaitTime;
    private volatile boolean running = false;


    private synchronized void createSessionsIfNeeded() {
        if (opcOperations != null && opcOperations.resetStale()) {
            sessions = new HashMap<>();
            tagInfoMap.entrySet().stream().collect(Collectors.groupingBy(entry -> entry.getValue().refreshPeriodMillis))
                    .forEach((a, b) -> {
                        OpcDaSessionProfile sessionProfile = new OpcDaSessionProfile().withDirectRead(directRead)
                                .withRefreshPeriodMillis(a);
                        OpcDaSession session = opcOperations.createSession(sessionProfile);
                        b.forEach(c -> sessions.put(c.getKey(), session));
                    });
        }
    }


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

        if (properties.containsKey(OpcDaSourceConnector.PROPERTY_SOCKET_TIMEOUT)) {
            ret.setSocketTimeout(Duration.ofMillis(Long.parseLong(properties.get(OpcDaSourceConnector.PROPERTY_SOCKET_TIMEOUT))));
        }
        return ret;
    }

    private SchemaAndValue convertToNativeType(final Object value) {

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
        opcOperations = new SmartOpcOperations<>(new OpcDaOperations());
        OpcDaConnectionProfile connectionProfile = propertiesToConnectionProfile(props);
        tags = props.get(OpcDaSourceConnector.PROPERTY_TAGS).split(",");
        host = connectionProfile.getHost();
        domain = connectionProfile.getDomain() != null ? connectionProfile.getDomain() : "";
        defaultRefreshPeriodMillis = Long.parseLong(props.get(OpcDaSourceConnector.PROPERTY_DEFAULT_REFRESH_PERIOD));
        directRead = Boolean.parseBoolean(props.get(OpcDaSourceConnector.PROPERTY_DIRECT_READ));
        tagInfoMap = Arrays.stream(tags).map(t -> new TagInfo(t, defaultRefreshPeriodMillis))
                .collect(Collectors.toMap(t -> t.name, Function.identity()));
        opcOperations.connect(connectionProfile);
        if (!opcOperations.awaitConnected()) {
            throw new ConnectException("Unable to connect");
        }
        logger.info("Started OPC-DA task for tags {}", (Object) tags);
        minWaitTime = Math.max(10, gcd(tagInfoMap.values().stream().mapToLong(t -> t.refreshPeriodMillis).toArray()));
        tagReadingQueue = new HashSet<>();
        running = true;
        executorService = Executors.newSingleThreadScheduledExecutor();
        tagInfoMap.forEach((k, v) -> executorService.scheduleAtFixedRate(() -> {
            try {
                lock.lock();
                tagReadingQueue.add(k);
            } finally {
                lock.unlock();
            }
        }, 0, v.refreshPeriodMillis, TimeUnit.MILLISECONDS));

        executorService.scheduleAtFixedRate(() -> {
            try {
                Set<String> tagsToRead;
                try {
                    lock.lock();
                    tagsToRead = new HashSet<>(tagReadingQueue);
                    tagReadingQueue.clear();
                } finally {
                    lock.unlock();
                }
                if (tagsToRead.isEmpty()) {
                    return;
                }
                createSessionsIfNeeded();
                Map<OpcDaSession, List<String>> sessionTags =
                        tagsToRead.stream().collect(Collectors.groupingBy(sessions::get));
                logger.info("Reading {}", sessionTags);
                sessionTags.entrySet().parallelStream()
                        .map(entry -> entry.getKey().read(entry.getValue().toArray(new String[entry.getValue().size()])))
                        .flatMap(Collection::stream)
                        .map(opcData -> {
                                    SchemaAndValue tmp = convertToNativeType(opcData.getValue());
                                    Schema valueSchema = buildSchema(tmp.schema());
                                    TagInfo meta = tagInfoMap.get(opcData.getTag());
                                    Struct value = new Struct(valueSchema)
                                            .put(OpcDaFields.TIMESTAMP, opcData.getTimestamp().toEpochMilli())
                                            .put(OpcDaFields.TAG_NAME, opcData.getTag())
                                            .put(OpcDaFields.QUALITY, opcData.getQuality())
                                            .put(OpcDaFields.UPDATE_PERIOD, meta.refreshPeriodMillis)
                                            .put(OpcDaFields.TAG_GROUP, meta.group)
                                            .put(OpcDaFields.OPC_SERVER_HOST, host)
                                            .put(OpcDaFields.OPC_SERVER_DOMAIN, domain);

                                    if (tmp.value() != null) {
                                        value = value.put(OpcDaFields.VALUE, tmp.value());
                                    }
                                    if (opcData.getErrorCode().isPresent()) {
                                        value.put(OpcDaFields.ERROR_CODE, opcData.getErrorCode().get());
                                    }

                                    Map<String, String> partition = new HashMap<>();
                                    partition.put(OpcDaFields.TAG_NAME, opcData.getTag());
                                    partition.put(OpcDaFields.OPC_SERVER_DOMAIN, domain);
                                    partition.put(OpcDaFields.OPC_SERVER_HOST, host);


                                    return new SourceRecord(
                                            partition,
                                            Collections.singletonMap(OpcDaFields.TIMESTAMP, opcData.getTimestamp().toEpochMilli()),
                                            "",
                                            SchemaBuilder.STRING_SCHEMA,
                                            domain + "|" + host + "|" + opcData.getTag(),
                                            valueSchema,
                                            value);
                                }
                        ).forEach(sourceRecord -> {
                    try {
                        transferQueue.put(sourceRecord);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Interrupted", e);
                    }
                });
            } catch (Exception e) {
                logger.error("Got exception while reading tags", e);
            }
        }, 0L, minWaitTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (transferQueue.isEmpty()) {
            Thread.sleep(minWaitTime);
        }
        List<SourceRecord> ret = new ArrayList<>();
        transferQueue.drainTo(ret);
        return ret;
    }

    @Override
    public void stop() {
        running = false;
        if (executorService != null) {
            executorService.shutdown();
            executorService = null;
        }

        if (opcOperations != null) {
            opcOperations.disconnect();
            opcOperations.awaitDisconnected();
        }
        //session are automatically cleaned up and detached when the connection is closed.
        sessions = null;
        transferQueue = null;
        tagReadingQueue = null;
        tagInfoMap = null;
        logger.info("Stopped OPC-DA task for tags {}", (Object) tags);
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    /**
     * GCD recursive version
     *
     * @param x dividend
     * @param y divisor
     * @return
     */
    private static long gcdInternal(long x, long y) {
        return (y == 0) ? x : gcdInternal(y, x % y);
    }

    /**
     * Great common divisor (An elegant way to do it with a lambda).
     *
     * @param numbers list of number
     * @return the GCD.
     */
    private static long gcd(long... numbers) {
        return Arrays.stream(numbers).reduce(0, (x, y) -> (y == 0) ? x : gcdInternal(y, x % y));
    }


}
