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

package com.hurence.logisland.util.kafkaconnect.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Source polling thread.
 */
class SourceThread implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceTask.class);

    private final SourceTask task;
    private final SQLContext sqlContext;
    private final Map<String, String> config;
    private final SharedSourceTaskContext sharedSourceTaskContext;
    private final AtomicBoolean running = new AtomicBoolean(false);


    public SourceThread(SourceTask task, SQLContext sqlContext, Map<String, String> config, SharedSourceTaskContext sharedSourceTaskContext) {
        this.sqlContext = sqlContext;
        this.task = task;
        this.config = Collections.unmodifiableMap(config);
        this.sharedSourceTaskContext = sharedSourceTaskContext;
        task.initialize(sharedSourceTaskContext);
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                List<SourceRecord> records = task.poll();
                if (records != null) {
                    records.forEach(sourceRecord -> sharedSourceTaskContext.offer(sourceRecord, LongOffset.apply(sourceRecord.sourceOffset().hashCode()), task));
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                LOGGER.warn("Unexpected error occurred while polling task " + task.getClass().getCanonicalName(), e);
            }
        }
    }

    public SourceThread start() {
        try {
            task.start(config);
            running.set(true);
        } catch (Throwable t) {
            LOGGER.error("Unable to start task " + task.getClass().getCanonicalName(), t);
            try {
                task.stop();
            } catch (Throwable tt) {
                //swallow
            }
        }

        return this;
    }

    public void stop() {
        running.set(false);

    }
}
