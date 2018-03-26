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

package com.hurence.logisland.engine.kc;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * BinarySourceTask is a Task that reads changes from a directory for storage
 * new binary detected files in Kafka.
 *
 * @author Alex Piermatteo
 */
public class BinarySourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(BinarySourceTask.class);

    private static Schema schema = null;
    private String schemaName;
    private String topic;
    private String filename_path;
    private boolean ran = false;


    @Override
    public String version() {
        return new BinarySourceConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the Task.
     *
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {

        schemaName = props.get(BinarySourceConnector.SCHEMA_NAME);
        if (schemaName == null)
            throw new ConnectException("config schema.name null");
        topic = props.get(BinarySourceConnector.TOPIC);
        if (topic == null)
            throw new ConnectException("config topic null");

        filename_path = props.get(BinarySourceConnector.FILE_PATH);
        if (filename_path == null || filename_path.isEmpty())
            throw new ConnectException("missing filename.path");


        log.trace("Creating schema");
        schema = SchemaBuilder
                .struct().name(schemaName)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("binary", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
    }


    /**
     * Poll this BinarySourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptException {
        if (!ran) {
            ran = true;
            List<SourceRecord> records = new ArrayList<>();

            File file = new File(filename_path);
            // creates the record
            // no need to save offsets
            SourceRecord record = create_binary_record(file);
            records.add(record);
            this.stop();


            return records;
        }
        return null;
    }


    /**
     * Create a new SourceRecord from a File
     *
     * @return a source records
     */
    private SourceRecord create_binary_record(File file) {

        byte[] data = null;
        try {
            //transform file to byte[]
            Path path = Paths.get(file.getPath());
            data = Files.readAllBytes(path);
            log.error(String.valueOf(data.length));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // creates the structured message
        Struct messageStruct = new Struct(schema);
        messageStruct.put("name", file.getName());
        messageStruct.put("binary", data);
        // creates the record
        // no need to save offsets
        return new SourceRecord(Collections.singletonMap("file_binary", 0), Collections.singletonMap("0", 0), topic, 0, messageStruct.schema(), messageStruct);
    }


    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
    }

}