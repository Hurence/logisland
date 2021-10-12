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
package com.hurence.logisland.connect.spooldir;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.data.Parser;
import com.github.jcustenborder.kafka.connect.utils.data.type.DateTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TimeTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TimestampTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TypeParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.hurence.logisland.connect.spooldir.modele.DelayedFile;
import com.hurence.logisland.utils.LinkedHashSetBlockingQueue;
import com.hurence.logisland.utils.SynchronizedFileLister;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

public abstract class SpoolDirSourceTask<CONF extends SpoolDirSourceConnectorConfig> extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(SpoolDirSourceTask.class);
    protected Parser parser;
    protected Map<String, ?> sourcePartition;
    CONF config;
    Stopwatch processingTime = Stopwatch.createStarted();
    //those 4 properties are set up in openNextFileAndConfigureIt
    private File inputFile;
    private InputStream inputStream;
    private long inputFileModifiedTime;
    private Map<String, String> metadata;


    private boolean currentFileFinished = false;
    private boolean currentFileInError = false;
    private boolean lastChance = false;
    private boolean firstPoll = true;
    private SynchronizedFileLister fileLister;
    final DelayQueue<DelayedFile> errorFileQueue = new DelayQueue<>();


    private static void checkDirectory(String key, File directoryPath) {
        if (log.isInfoEnabled()) {
            log.info("Checking if directory {} '{}' exists.",
                    key,
                    directoryPath
            );
        }

        String errorMessage = String.format(
                "Directory for '%s' '%s' does not exist ",
                key,
                directoryPath
        );

        if (!directoryPath.isDirectory()) {
            throw new ConnectException(
                    errorMessage,
                    new FileNotFoundException(directoryPath.getAbsolutePath())
            );
        }

        if (log.isInfoEnabled()) {
            log.info("Checking to ensure {} '{}' is writable ", key, directoryPath);
        }

        errorMessage = String.format(
                "Directory for '%s' '%s' it not writable.",
                key,
                directoryPath
        );

        File temporaryFile = null;

        try {
            temporaryFile = File.createTempFile(".permission", ".testing", directoryPath);
        } catch (IOException ex) {
            throw new ConnectException(
                    errorMessage,
                    ex
            );
        } finally {
            try {
                if (null != temporaryFile && temporaryFile.exists()) {
                    Preconditions.checkState(temporaryFile.delete(), "Unable to delete temp file in %s", directoryPath);
                }
            } catch (Exception ex) {
                if (log.isWarnEnabled()) {
                    log.warn("Exception thrown while deleting {}.", temporaryFile, ex);
                }
            }
        }
    }

    protected abstract CONF config(Map<String, ?> settings);

    protected abstract void configure(InputStream inputStream, Map<String, String> metadata, Long lastOffset) throws IOException;

    protected abstract List<SourceRecord> process() throws IOException;

    protected abstract long recordOffset();

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }

    @Override
    public void start(Map<String, String> settings) {
        this.config = config(settings);
        this.fileLister = SynchronizedFileLister.getInstance(config.inputPath, config.inputFilenameFilter, config.minimumFileAgeMS, config.processingFileExtension);

        checkDirectory(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, this.config.inputPath);
        checkDirectory(SpoolDirSourceConnectorConfig.FINISHED_PATH_CONFIG, this.config.finishedPath);
        checkDirectory(SpoolDirSourceConnectorConfig.ERROR_PATH_CONFIG, this.config.errorPath);

        this.parser = new Parser();
        Map<Schema, TypeParser> dateTypeParsers = ImmutableMap.of(
                Timestamp.SCHEMA, new TimestampTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats),
                Date.SCHEMA, new DateTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats),
                Time.SCHEMA, new TimeTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats)
        );

        for (Map.Entry<Schema, TypeParser> kvp : dateTypeParsers.entrySet()) {
            this.parser.registerTypeParser(kvp.getKey(), kvp.getValue());
        }
        config.logUnused();
        log.info("Started source task : {}", SpoolDirSourceTask.class.getCanonicalName());
    }

    @Override
    public void stop() {
        if (this.inputStream != null) {
            try {
                this.inputStream.close();
            } catch (IOException e) {
                log.error("Exception thrown while closing inputstream of class : " +  this.getClass().getCanonicalName(), e);
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.debug("Polling for new data (files)");
        if (fileLister.needToUpdateList()) {
            fileLister.lock();
            try {
                if (fileLister.needToUpdateList()) {
                    fileLister.updateList();
                }
            } finally {
                fileLister.unlock();
            }
        }
        if (firstPoll) {
            firstPoll = false;
            if (!openNextFileAndConfigureIt()) {//no file available
                log.info("No more files available. Sleeping {} ms.", this.config.emptyPollWaitMs * 2);
                Thread.sleep(this.config.emptyPollWaitMs * 2);
                return Collections.emptyList();
            }
        }
        if (currentFileFinished) {
            if (!openNextFileAndConfigureIt()) {//no file available
                log.info("No more files available. Sleeping {} ms.", this.config.emptyPollWaitMs * 2);
                Thread.sleep(this.config.emptyPollWaitMs * 2);
                return Collections.emptyList();
            }
        }

        List<SourceRecord> results = read();
        this.currentFileFinished = results.isEmpty();
        if (currentFileFinished) {
            if (currentFileInError) {
                if (lastChance) {
                    moveCurrentFileToErrorFolder();
                } else {
                    log.error("parsing file {} resulted in an error. Will try once more time after a delay", this.inputFile);
                    errorFileQueue.add(new DelayedFile(this.inputFile, this.config.delayOnErrorMs, TimeUnit.MILLISECONDS));
                }
                lastChance = false;
                currentFileInError = false;
            } else {
                moveCurrentFileToFinishedFolder();
            }
            log.info("Current file is ended. Sleeping {} ms before processing next file.", this.config.emptyPollWaitMs);
            Thread.sleep(this.config.emptyPollWaitMs);
        } else {
            log.trace("read() returning {} result(s)", results.size());
        }

        return results;
    }

    public void moveCurrentFileToErrorFolder() {
        try {
            log.info("Moving file {} to error folder {}", this.inputFile, this.config.errorPath);
            fileLister.moveTo(this.inputFile, this.config.inputPath, this.config.errorPath);
        } catch (IOException ex0) {
            log.error(
                    String.format("Exception thrown while moving file %s to error folder %s",
                            this.inputFile, this.config.errorPath),
                    ex0);
        }
    }

    public void moveCurrentFileToFinishedFolder() {
        log.info("Moving file {} to finished folder {}", this.inputFile, this.config.finishedPath);
        try {
            fileLister.moveTo(this.inputFile, this.config.inputPath, this.config.finishedPath);
        } catch (IOException e) {
            log.error(
                    String.format("Exception encountered moving file %s to finished folder %s",
                            this.inputFile, this.config.finishedPath),
                    e);
        }
    }

    /**
     *
     * @return true if there is a next file otherwise return false
     */
    private boolean openNextFileAndConfigureIt() {
        DelayedFile failedFile = errorFileQueue.poll();
        if (failedFile != null) {
            this.inputFile = failedFile.getFailedFile();
            lastChance = true;
            log.info("Opening {}, this file produced an error last time trying to parse it", this.inputFile);
        } else {
            this.inputFile = fileLister.take();
        }
        if (null == this.inputFile) {
            return false;
        }
        this.metadata = ImmutableMap.of();
        this.inputFileModifiedTime = this.inputFile.lastModified();

        try {
            this.sourcePartition = ImmutableMap.of(
                    "fileName", this.inputFile.getName()
            );
            log.info("Opening {}", this.inputFile);
            Long lastOffset = null;
            log.debug("looking up offset for {}", this.sourcePartition);
            if (this.context != null) {
                Map<String, Object> offset = this.context.offsetStorageReader().offset(this.sourcePartition);
                if (null != offset && !offset.isEmpty()) {
                    Number number = (Number) offset.get("offset");
                    lastOffset = number.longValue();
                }
            }
            if (this.inputStream != null) {
                try {
                    this.inputStream.close();
                } catch (Exception ex) {
                    log.error("Exception during inputStream close", ex);
                }
            }
            this.inputStream = new FileInputStream(this.inputFile);
            configure(this.inputStream, this.metadata, lastOffset);
        } catch (Exception ex) {
            log.error("Exception during inputStream configuration", ex);
            throw new ConnectException(ex);
        }
        processingTime.reset();
        processingTime.start();
        return true;
    }

    private List<SourceRecord> read() {
        try {
            return process();
        } catch (Exception ex) {
            log.error(
                    String.format("Exception encountered processing line %s of %s.",
                            recordOffset(), this.inputFile),
                    ex);
            currentFileInError = true;
            if (this.config.haltOnError) {
                throw new ConnectException(ex);
            } else {
                return new ArrayList<>();
            }
        }
    }

    protected void addRecord(List<SourceRecord> records, Struct keyStruct, Struct valueStruct) {
        Map<String, ?> sourceOffset = ImmutableMap.of(
                "offset",
                recordOffset()
        );

        log.trace("addRecord() - {}", sourceOffset);
        if (this.config.hasKeyMetadataField && null != keyStruct) {
            keyStruct.put(this.config.keyMetadataField, this.metadata);
        }

        if (this.config.hasvalueMetadataField && null != valueStruct) {
            valueStruct.put(this.config.valueMetadataField, this.metadata);
        }

        final Long timestamp;

        switch (this.config.timestampMode) {
            case FIELD:
                log.debug("addRecord() - Reading date from timestamp field '{}'", this.config.timestampField);
                java.util.Date date = (java.util.Date) valueStruct.get(this.config.timestampField);
                timestamp = date.getTime();
                break;
            case FILE_TIME:
                timestamp = this.inputFileModifiedTime;
                break;
            case PROCESS_TIME:
                timestamp = null;
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported timestamp mode. %s", this.config.timestampMode)
                );
        }


        //TODO: Comeback and add timestamp support.

        SourceRecord sourceRecord = new SourceRecord(
                this.sourcePartition,
                sourceOffset,
                this.config.topic,
                null,
                this.config.keySchema,
                keyStruct,
                this.config.valueSchema,
                valueStruct,
                timestamp
        );
        records.add(sourceRecord);
    }
}