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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BinarySourceConnector implements the connector interface
 * to write on Kafka binary files
 *
 * @author Alex Piermatteo
 */
public class BinarySourceConnector extends SourceConnector {


    public static final String SCHEMA_NAME = "schema.name";
    public static final String TOPIC = "topic";
    public static final String FILE_PATH = "filename.path";

    private String schema_name;
    private String topic;
    private String filename_path;


    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }


    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {

        schema_name = props.get(SCHEMA_NAME);
        if (schema_name == null || schema_name.isEmpty())
            throw new ConnectException("missing schema.name");
        topic = props.get(TOPIC);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("missing topic");

        filename_path = props.get(FILE_PATH);
        if (filename_path == null || filename_path.isEmpty())
            throw new ConnectException("missing filename.path");


    }


    /**
     * Returns the Task implementation for this Connector.
     *
     * @return tha Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return BinarySourceTask.class;
    }


    /**
     * Returns a set of configurations for the Task based on the current configuration.
     * It always creates a single set of configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for the Task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();

            config.put(FILE_PATH, filename_path);
            config.put(SCHEMA_NAME, schema_name);
            config.put(TOPIC, topic);
            configs.add(config);
        }
        return configs;
    }


    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FILE_PATH, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "File Path")
                .define(SCHEMA_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Schema name")
                .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Output topic");
    }

}