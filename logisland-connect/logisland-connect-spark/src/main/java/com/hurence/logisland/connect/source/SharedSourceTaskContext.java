/*
 * Copyright (C) 2018 Hurence (support@hurence.com)
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
 *
 */

package com.hurence.logisland.connect.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.spark.sql.execution.streaming.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A {@link SourceTaskContext} shared among all task spawned by a connector.
 * <p>
 * An instance of this class is regularly polled by spark structured stream engine.
 */
public class SharedSourceTaskContext implements SourceTaskContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(SharedSourceTaskContext.class);


    private final OffsetStorageReader offsetStorageReader;
    private final OffsetStorageWriter offsetStorageWriter;
    private final Deque<Tuple3<SourceRecord, Offset, SourceTask>> buffer = new LinkedList<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();


    /**
     * Create a new instance.
     *
     * @param offsetStorageReader the offset reader (managed by creating class).
     * @param offsetStorageWriter the offset writer (managed by the creating class)
     */
    public SharedSourceTaskContext(OffsetStorageReader offsetStorageReader, OffsetStorageWriter offsetStorageWriter) {
        this.offsetStorageReader = offsetStorageReader;
        this.offsetStorageWriter = offsetStorageWriter;

    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return offsetStorageReader;
    }

    /**
     * Fetch last offset available.
     *
     * @return
     */
    public Optional<Offset> lastOffset() {
        Lock lock = rwLock.readLock();
        try {
            lock.lock();
            return Optional.ofNullable(buffer.isEmpty() ? null : buffer.getLast()._2());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Read the received data according to provided offsets.
     *
     * @param from the optional starting offset. If missing data will be fetched since the beginning of available one.
     * @param to   the mandatory ending offset.
     * @return the {@link SourceRecord} that have been read.
     */
    public Collection<SourceRecord> read(Optional<Offset> from, Offset to) {
        Lock lock = rwLock.readLock();
        try {
            lock.lock();
            boolean started = false;
            Collection<SourceRecord> ret = new ArrayList<>();
            for (Tuple3<SourceRecord, Offset, SourceTask> current : buffer) {
                Offset lo = current._2();
                if (started || !from.isPresent() || from.get().equals(lo)) {
                    started = true;
                    ret.add(current._1());
                }
                if (to.equals(lo)) {
                    break;
                }
            }
            return ret;
        } finally {
            lock.unlock();

        }
    }

    /**
     * Enqueue a new record emitted by a {@link SourceTask}
     *
     * @param record  the {@link SourceRecord} coming from the connector
     * @param offset  the corresponding {@link Offset}
     * @param emitter the record emitter.
     */
    public void offer(SourceRecord record, Offset offset, SourceTask emitter) {
        Lock lock = rwLock.writeLock();
        try {
            lock.lock();
            buffer.addLast(Tuple3.<SourceRecord, Offset, SourceTask>apply(record, offset, emitter));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Confirms that data read since offset endOffset has been successfully handled by the streaming engine.
     *
     * @param endOffset the last offset read and committed by the spark engine.
     */
    public void commit(Offset endOffset) {
        Lock lock = rwLock.readLock();
        try {
            lock.lock();
            Tuple3<SourceRecord, Offset, SourceTask> sr = buffer.stream()
                    .filter(item -> endOffset.equals(item._2()))
                    .findFirst().get();
            offsetStorageWriter.offset(sr._1().sourcePartition(), sr._1().sourceOffset());
            if (offsetStorageWriter.beginFlush()) {
                offsetStorageWriter.doFlush((error, result) -> {
                    if (error == null) {
                        Lock ll = rwLock.writeLock();
                        try {
                            ll.lock();
                            while (!buffer.isEmpty()) {
                                Tuple3<SourceRecord, Offset, SourceTask> current = buffer.removeFirst();
                                if (current._3() != null) {
                                    try {
                                        current._3().commitRecord(current._1());
                                    } catch (InterruptedException e) {
                                        LOGGER.warn("Interrupted while committing", e);
                                    }
                                }
                                if (endOffset.equals(current._2())) {
                                    break;
                                }
                            }
                        } finally {
                            ll.unlock();
                        }
                    } else {
                        LOGGER.error("Unable to commit records till source offset " + endOffset, error);
                    }
                });
            }
        } finally {
            lock.unlock();
        }

    }

    /**
     * Clean up buffered data.
     */
    public void clean() {
        Lock lock = rwLock.writeLock();
        try {
            lock.lock();
            buffer.clear();
        } finally {
            lock.unlock();
        }
    }
}
