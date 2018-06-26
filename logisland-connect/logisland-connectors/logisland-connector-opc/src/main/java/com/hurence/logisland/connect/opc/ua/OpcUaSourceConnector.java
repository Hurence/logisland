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

package com.hurence.logisland.connect.opc.ua;

import com.hurence.logisland.connect.opc.CommonUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * OPC-UA source connector.
 *
 * @author amarziali
 */
public class OpcUaSourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(OpcUaSourceConnector.class);

    private Map<String, ConfigValue> configValues;

    public static final String PROPERTY_URI = "uri";
    public static final String PROPERTY_AUTH_USER = "auth.user";
    public static final String PROPERTY_AUTH_PASSWORD = "auth.password";
    public static final String PROPERTY_TAGS = "tags";
    public static final String PROPERTY_SOCKET_TIMEOUT = "socketTimeoutMillis";
    public static final String PROPERTY_DEFAULT_REFRESH_PERIOD = "defaultRefreshPeriodMillis";
    public static final String PROPERTY_DATA_PUBLICATION_PERIOD = "dataPublicationPeriodMillis";


    /**
     * The configuration.
     */
    private static final ConfigDef CONFIG = new ConfigDef()
            .define(PROPERTY_URI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The OPC-UA server uri")
            .define(PROPERTY_AUTH_USER, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "The logon user")
            .define(PROPERTY_AUTH_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "The logon password")
            .define(PROPERTY_TAGS, ConfigDef.Type.LIST, Collections.emptyList(), (name, value) -> {
                if (value == null) {
                    throw new ConfigException("Cannot be null");
                }
                List<String> list = (List<String>) value;
                for (String s : list) {
                    if (CommonUtils.validateTagFormat(s)) {
                        throw new ConfigException("Tag list should be like [tag_name]:[refresh_period_millis] with optional refresh period");
                    }
                }
            }, ConfigDef.Importance.HIGH, "The tags to subscribe to following format tagname:refresh_period_millis. E.g. myTag:1000")
            .define(PROPERTY_SOCKET_TIMEOUT, ConfigDef.Type.LONG, ConfigDef.Importance.LOW, "The socket timeout")
            .define(PROPERTY_DEFAULT_REFRESH_PERIOD, ConfigDef.Type.LONG, 1000, ConfigDef.Importance.LOW, "The default data refresh period in milliseconds")
            .define(PROPERTY_DATA_PUBLICATION_PERIOD, ConfigDef.Type.LONG, 1000, ConfigDef.Importance.LOW, "The data publication window in milliseconds");


    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        //shallow copy
        configValues = config().validate(props).stream().collect(Collectors.toMap(ConfigValue::name, Function.identity()));
        logger.info("Starting OPC-UA connector (version {}) on server {} reading tags {}", version(),
                configValues.get(PROPERTY_URI).value(), configValues.get(PROPERTY_TAGS).value());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OpcUaSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> ret = configValues.entrySet().stream()
                .filter(a -> a.getValue().value() != null)
                .collect(Collectors.toMap(a -> a.getKey(), a -> a.getValue().value().toString()));
        ret.put(PROPERTY_TAGS, ((List<String>) configValues.get(PROPERTY_TAGS).value()).stream().collect(Collectors.joining(",")));
        return Collections.singletonList(ret);
    }

    @Override
    public void stop() {
        logger.info("Stopping OPC-UA connector (version {}) on server {}", version(), configValues.get(PROPERTY_URI).value());
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }
}
