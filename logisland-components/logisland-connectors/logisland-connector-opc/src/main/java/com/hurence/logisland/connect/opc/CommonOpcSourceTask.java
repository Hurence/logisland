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

import com.hurence.opc.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.jooq.lambda.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * OPC base worker task.
 */
public abstract class CommonOpcSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(CommonOpcSourceTask.class);
    private SmartOpcOperations opcOperations;
    private TransferQueue<Pair<Instant, OpcData>> transferQueue;
    private OpcSession session;
    private Map<String, TagInfo> tagInfoMap;
    private ScheduledExecutorService pollingScheduler;
    private ExecutorService streamingThread;
    private String host;
    private long minWaitTime;
    private final AtomicBoolean running = new AtomicBoolean();


    private synchronized void createSessionIfNeeded() {
        if (opcOperations != null && opcOperations.awaitConnected()) {
            session = opcOperations.createSession(createSessionProfile());
        } else {
            safeCloseSession();
        }

    }

    /**
     * Subclasses should use this hook to set their specific properties.
     *
     * @param properties
     */
    protected abstract void setConfigurationProperties(Map<String, String> properties);

    /**
     * Subclasses to create their specific session profile.
     *
     * @return
     */
    protected abstract SessionProfile createSessionProfile();

    /**
     * Subclasses to create their specific connection profile.
     *
     * @return
     */
    protected abstract ConnectionProfile createConnectionProfile();

    /**
     * Subclasses to create their specific {@link OpcOperations} instance.
     * @return
     */
    protected abstract OpcOperations createOpcOperations();

    /**
     * Indicate whenever the server can do sompling on a subscription.
     * Usually it is false for synchronous OPC-DA but generally true for OPC-UA.
     *
     * @return
     */
    protected boolean hasServerSideSampling() {
        return false;
    }


    @Override
    public void start(Map<String, String> props) {
        setConfigurationProperties(props);

        transferQueue = new LinkedTransferQueue<>();
        opcOperations = new SmartOpcOperations<>(createOpcOperations());
        ConnectionProfile connectionProfile = createConnectionProfile();
        host = connectionProfile.getConnectionUri().getHost();
        tagInfoMap = CommonUtils.parseTagsFromProperties(props)
                .stream().collect(Collectors.toMap(TagInfo::getTagId, Function.identity()));
        minWaitTime = Math.min(10, tagInfoMap.values().stream().map(TagInfo::getSamplingInterval).mapToLong(Duration::toMillis).min().getAsLong());
        opcOperations.connect(connectionProfile);
        if (!opcOperations.awaitConnected()) {
            throw new ConnectException("Unable to connect");
        }

        //set up polling source emission
        pollingScheduler = Executors.newSingleThreadScheduledExecutor();
        streamingThread = Executors.newSingleThreadExecutor();
        Map<Duration, List<TagInfo>> pollingMap = tagInfoMap.values().stream().filter(tagInfo -> StreamingMode.POLL.equals(tagInfo.getStreamingMode()))
                .collect(Collectors.groupingBy(TagInfo::getSamplingInterval));
        final Map<String, OpcData> lastValues = Collections.synchronizedMap(new HashMap<>());
        pollingMap.forEach((k, v) ->
                pollingScheduler.scheduleAtFixedRate(() -> {
                            final Instant now = Instant.now();
                            v.stream().map(TagInfo::getTagId).map(lastValues::get).filter(Functions.not(Objects::isNull))
                                    .map(data -> Pair.of(now, data)).forEach(transferQueue::add);

                        },
                        0, k.toNanos(), TimeUnit.NANOSECONDS)
        );
        //then subscribe for all
        final SubscriptionConfiguration subscriptionConfiguration = new SubscriptionConfiguration().withDefaultSamplingInterval(Duration.ofMillis(10_000));
        tagInfoMap.values().forEach(tagInfo -> subscriptionConfiguration.withTagSamplingIntervalForTag(tagInfo.getTagId(), tagInfo.getSamplingInterval()));
        running.set(true);
        streamingThread.submit(() -> {
            while (running.get()) {
                try {
                    createSessionIfNeeded();
                    if (session == null) {
                        return;
                    }

                    session.stream(subscriptionConfiguration, tagInfoMap.keySet().toArray(new String[tagInfoMap.size()]))
                            .forEach(opcData -> {
                                if (tagInfoMap.get(opcData.getTag()).getStreamingMode().equals(StreamingMode.SUBSCRIBE)) {
                                    transferQueue.add(Pair.of(hasServerSideSampling() ? opcData.getTimestamp() : Instant.now(), opcData));
                                } else {
                                    lastValues.put(opcData.getTag(), opcData);
                                }
                            });
                } catch (Exception e) {
                    if (running.get()) {
                        logger.warn("Stream interrupted while reading from " + host, e);
                        safeCloseSession();
                        lastValues.clear();

                    }
                }
            }
        });

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (transferQueue.isEmpty()) {
            Thread.sleep(minWaitTime);
        }
        List<Pair<Instant, OpcData>> ret = new ArrayList<>();
        transferQueue.drainTo(ret);
        return ret.stream()
                .map(tuple -> {
                    OpcData opcData = tuple.getValue();
                    SchemaAndValue tmp = CommonUtils.convertToNativeType(opcData.getValue());
                    Schema valueSchema = CommonUtils.buildSchema(tmp.schema());
                    TagInfo meta = tagInfoMap.get(opcData.getTag());
                    Map<String, Object> additionalInfo = new HashMap<>();
                    additionalInfo.put(OpcRecordFields.OPC_SERVER, host);
                    Struct value = CommonUtils.mapToConnectObject(opcData,
                            tuple.getKey(),
                            meta,
                            valueSchema,
                            tmp,
                            additionalInfo);

                    Map<String, String> partition = new HashMap<>();
                    partition.put(OpcRecordFields.TAG_ID, opcData.getTag());
                    partition.put(OpcRecordFields.OPC_SERVER, host);


                    return new SourceRecord(
                            partition,
                            Collections.singletonMap(OpcRecordFields.SAMPLED_TIMESTAMP, tuple.getKey().toEpochMilli()),
                            "",
                            SchemaBuilder.STRING_SCHEMA,
                            host + "|" + opcData.getTag(),
                            valueSchema,
                            value);
                }).collect(Collectors.toList());
    }


    @Override
    public void stop() {
        running.set(false);
        if (pollingScheduler != null) {
            pollingScheduler.shutdown();
            pollingScheduler = null;
        }
        if (streamingThread != null) {
            streamingThread.shutdown();
            streamingThread = null;
        }

        safeCloseSession();

        if (opcOperations != null) {
            try {
                opcOperations.disconnect();
                opcOperations.awaitDisconnected();
            } catch (Exception e) {
                logger.warn("Unable to properly disconnect the client", e);
            }
        }
        transferQueue = null;
        tagInfoMap = null;
        logger.info("Stopped OPC task for server {}", host);
    }

    private synchronized void safeCloseSession() {
        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                logger.warn("Unable to properly close the session", e);
            } finally {
                session = null;
            }
        }
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }


}
