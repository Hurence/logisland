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

package com.hurence.logisland.connect.opc.da;

import com.hurence.logisland.connect.opc.CommonUtils;
import com.hurence.logisland.connect.opc.OpcRecordFields;
import com.hurence.logisland.connect.opc.SmartOpcOperations;
import com.hurence.logisland.connect.opc.TagInfo;
import com.hurence.opc.ConnectionState;
import com.hurence.opc.OpcTagInfo;
import com.hurence.opc.auth.UsernamePasswordCredentials;
import com.hurence.opc.da.OpcDaConnectionProfile;
import com.hurence.opc.da.OpcDaSession;
import com.hurence.opc.da.OpcDaSessionProfile;
import com.hurence.opc.da.OpcDaTemplate;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
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

    private static final Logger logger = LoggerFactory.getLogger(OpcDaSourceTask.class);
    private SmartOpcOperations<OpcDaConnectionProfile, OpcDaSessionProfile, OpcDaSession> opcOperations;
    private TransferQueue<SourceRecord> transferQueue;
    private Lock lock = new ReentrantLock();
    private String tags[];
    private Map<String, OpcDaSession> sessions;
    private Map<String, TagInfo> tagInfoMap;
    private Set<String> tagReadingQueue;
    private ScheduledExecutorService executorService;
    private String domain;
    private String host;
    private boolean directRead;
    private long defaultRefreshPeriodMillis;
    private long minWaitTime;

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

    private synchronized void createSessionsIfNeeded() {
        if (opcOperations != null && (opcOperations.resetStale() || sessions == null)) {
            sessions = new HashMap<>();
            tagInfoMap.entrySet().stream().collect(Collectors.groupingBy(entry -> entry.getValue().getRefreshPeriodMillis()))
                    .forEach((a, b) -> {
                        OpcDaSessionProfile sessionProfile = new OpcDaSessionProfile().withDirectRead(directRead)
                                .withRefreshPeriod(Duration.ofMillis(a));
                        OpcDaSession session = opcOperations.createSession(sessionProfile);
                        b.forEach(c -> sessions.put(c.getKey(), session));
                    });
        }
    }

    private OpcDaConnectionProfile propertiesToConnectionProfile(Map<String, String> properties) {
        OpcDaConnectionProfile ret = new OpcDaConnectionProfile();
        StringBuilder uri = new StringBuilder(String.format("opc.da://%s",
                properties.get(OpcDaSourceConnector.PROPERTY_HOST)));
        ret.setComClsId(properties.get(OpcDaSourceConnector.PROPERTY_CLSID));
        ret.setComProgId(properties.get(OpcDaSourceConnector.PROPERTY_PROGID));
        if (properties.containsKey(OpcDaSourceConnector.PROPERTY_PORT)) {
            uri.append(String.format(":%d", Integer.parseInt(properties.get(OpcDaSourceConnector.PROPERTY_PORT))));
        }
        ret.setConnectionUri(URI.create(uri.toString()));
        ret.setCredentials(new UsernamePasswordCredentials()
                .withUser(properties.get(OpcDaSourceConnector.PROPERTY_USER))
                .withPassword(properties.get(OpcDaSourceConnector.PROPERTY_PASSWORD)));
        ret.setDomain(properties.get(OpcDaSourceConnector.PROPERTY_DOMAIN));

        if (properties.containsKey(OpcDaSourceConnector.PROPERTY_SOCKET_TIMEOUT)) {
            ret.setSocketTimeout(Duration.ofMillis(Long.parseLong(properties.get(OpcDaSourceConnector.PROPERTY_SOCKET_TIMEOUT))));
        }
        return ret;
    }


    @Override
    public void start(Map<String, String> props) {
        transferQueue = new LinkedTransferQueue<>();
        opcOperations = new SmartOpcOperations<>(new OpcDaTemplate());
        OpcDaConnectionProfile connectionProfile = propertiesToConnectionProfile(props);
        tags = props.get(OpcDaSourceConnector.PROPERTY_TAGS).split(",");
        domain = connectionProfile.getDomain() != null ? connectionProfile.getDomain() : "";
        host = connectionProfile.getConnectionUri().getHost();
        defaultRefreshPeriodMillis = Long.parseLong(props.get(OpcDaSourceConnector.PROPERTY_DEFAULT_REFRESH_PERIOD));
        directRead = Boolean.parseBoolean(props.get(OpcDaSourceConnector.PROPERTY_DIRECT_READ));

        opcOperations.connect(connectionProfile);
        if (!opcOperations.awaitConnected()) {
            throw new ConnectException("Unable to connect");
        }
        Map<String, OpcTagInfo> dictionary =
                opcOperations.fetchMetadata(Arrays.stream(tags).map(t -> CommonUtils.parseTag(t, defaultRefreshPeriodMillis).getKey())
                        .toArray(size -> new String[size]))
                        .stream()
                        .collect(Collectors.toMap(OpcTagInfo::getId, Function.identity()));
        tagInfoMap = Arrays.stream(tags).map(t -> new TagInfo(t, defaultRefreshPeriodMillis, dictionary))
                .collect(Collectors.toMap(t -> t.getTagInfo().getId(), Function.identity()));
        logger.info("Started OPC-DA task for tags {}", (Object) tags);
        minWaitTime = Math.max(10, gcd(tagInfoMap.values().stream().mapToLong(TagInfo::getRefreshPeriodMillis).toArray()));
        tagReadingQueue = new HashSet<>();
        executorService = Executors.newSingleThreadScheduledExecutor();
        tagInfoMap.forEach((k, v) -> executorService.scheduleAtFixedRate(() -> {
            try {
                lock.lock();
                tagReadingQueue.add(k);
            } finally {
                lock.unlock();
            }
        }, 0, v.getRefreshPeriodMillis(), TimeUnit.MILLISECONDS));

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
                logger.debug("Reading {}", sessionTags);
                sessionTags.entrySet().parallelStream()
                        .map(entry -> entry.getKey().read(entry.getValue().toArray(new String[entry.getValue().size()])))
                        .flatMap(Collection::stream)
                        .map(opcData -> {
                                    SchemaAndValue tmp = CommonUtils.convertToNativeType(opcData.getValue());
                                    Schema valueSchema = CommonUtils.buildSchema(tmp.schema());
                                    TagInfo meta = tagInfoMap.get(opcData.getTag());
                                    Map<String, Object> additionalInfo = new HashMap<>();
                                    additionalInfo.put(OpcRecordFields.OPC_SERVER_HOST, host);
                                    additionalInfo.put(OpcRecordFields.OPC_SERVER_DOMAIN, domain);
                                    Struct value = CommonUtils.mapToConnectObject(opcData,
                                            meta,
                                            valueSchema,
                                            tmp,
                                            additionalInfo);

                                    Map<String, String> partition = new HashMap<>();
                                    partition.put(OpcRecordFields.TAG_NAME, opcData.getTag());
                                    partition.put(OpcRecordFields.OPC_SERVER_DOMAIN, domain);
                                    partition.put(OpcRecordFields.OPC_SERVER_HOST, host);


                                    return new SourceRecord(
                                            partition,
                                            Collections.singletonMap(OpcRecordFields.TIMESTAMP, opcData.getTimestamp().toEpochMilli()),
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
                if (sessions != null && opcOperations != null && opcOperations.getConnectionState() == ConnectionState.CONNECTED) {
                    while (!sessions.isEmpty()) {

                        try {
                            sessions.remove(sessions.keySet().stream().findFirst().get()).close();
                        } catch (Exception e1) {
                            //swallow here
                        }
                    }
                }
                sessions = null;
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


}
