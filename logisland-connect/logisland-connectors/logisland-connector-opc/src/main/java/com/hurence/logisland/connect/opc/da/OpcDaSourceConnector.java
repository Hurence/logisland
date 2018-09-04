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
package com.hurence.logisland.connect.opc.da;

import com.hurence.logisland.connect.opc.CommonDefinitions;
import com.hurence.logisland.connect.opc.CommonUtils;
import org.apache.kafka.common.config.ConfigDef;
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
 * OPC-DA Connector.
 *
 * @author amarziali
 */
public class OpcDaSourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(OpcDaSourceConnector.class);

    private Map<String, ConfigValue> configValues;

    public static final String PROPERTY_SERVER_CLSID = "server.clsId";
    public static final String PROPERTY_SERVER_PROGID = "server.progId";
    public static final String PROPERTY_AUTH_NTLM_USER = "auth.ntlm.user";
    public static final String PROPERTY_AUTH_NTLM_PASSWORD = "auth.ntlm.password";
    public static final String PROPERTY_AUTH_NTLM_DOMAIN = "auth.ntlm.domain";

    public static final String PROPERTY_SESSION_REFRESH_PERIOD = "session.refreshPeriodMillis";
    public static final String PROPERTY_SESSION_DIRECT_READ = "session.directReadFromDevice";
    public static final String PROPERTY_TAGS_DATA_TYPE_OVERRIDE = "tags.type.override";


    /**
     * The configuration.
     */
    private static final ConfigDef CONFIG = CommonDefinitions.addCommonProperties(new ConfigDef())
            .define(PROPERTY_AUTH_NTLM_DOMAIN, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "The logon domain")
            .define(PROPERTY_AUTH_NTLM_USER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "The logon user")
            .define(PROPERTY_AUTH_NTLM_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "The logon password")
            .define(PROPERTY_SERVER_CLSID, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM,
                    "The CLSID of the OPC server COM component")
            .define(PROPERTY_SERVER_PROGID, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM,
                    "The Program ID of the OPC server COM component")
            .define(PROPERTY_SESSION_REFRESH_PERIOD, ConfigDef.Type.LONG, 10_000, ConfigDef.Importance.LOW,
                    "The opc group data refresh period in milliseconds")
            .define(PROPERTY_SESSION_DIRECT_READ, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                    "Use server cache or read directly from device")
            .define(PROPERTY_TAGS_DATA_TYPE_OVERRIDE, ConfigDef.Type.LIST, ConfigDef.Importance.LOW,
                    "If present will ask the opc server to override the data type with the one provided. " +
                            "Should be a short value supported by Variant");


    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        //shallow copy
        configValues = config().validate(props).stream().collect(Collectors.toMap(ConfigValue::name, Function.identity()));
        List<String> tags = (List<String>) configValues.get(CommonDefinitions.PROPERTY_TAGS_ID).value();
        List<String> freqs = (List<String>) configValues.get(CommonDefinitions.PROPERTY_TAGS_SAMPLING_RATE).value();
        List<String> modes = (List<String>) configValues.get(CommonDefinitions.PROPERTY_TAGS_STREAM_MODE).value();
        //validate the configs
        CommonUtils.validateTagConfig(tags, freqs, modes);
        if (configValues.get(PROPERTY_TAGS_DATA_TYPE_OVERRIDE).value() != null) {
            try {
                if (tags.size() != ((List<String>) configValues.get(PROPERTY_TAGS_DATA_TYPE_OVERRIDE).value()).stream()
                        .map(Short::parseShort).count()) {
                    throw new IllegalArgumentException("Provided list of data type override must match the length of provided tags");
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Provided list of data type override must contains only short values");
            }
        }
        //all right.. let's start
        logger.info("Starting OPC-DA connector (version {}) on server {} reading tags {}", version(),
                configValues.get(CommonDefinitions.PROPERTY_SERVER_URI).value(),
                configValues.get(CommonDefinitions.PROPERTY_TAGS_ID).value());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OpcDaSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> ret = configValues.entrySet().stream()
                .filter(a -> a.getValue().value() != null)
                .collect(Collectors.toMap(a -> a.getKey(), a -> a.getValue().value().toString()));
        ret.put(CommonDefinitions.PROPERTY_TAGS_ID, ((List<String>) configValues.get(CommonDefinitions.PROPERTY_TAGS_ID).value()).stream().collect(Collectors.joining(",")));
        ret.put(CommonDefinitions.PROPERTY_TAGS_STREAM_MODE, ((List<String>) configValues.get(CommonDefinitions.PROPERTY_TAGS_STREAM_MODE).value()).stream().collect(Collectors.joining(",")));
        ret.put(CommonDefinitions.PROPERTY_TAGS_SAMPLING_RATE, ((List<String>) configValues.get(CommonDefinitions.PROPERTY_TAGS_SAMPLING_RATE).value()).stream().collect(Collectors.joining(",")));
        if (configValues.get(PROPERTY_TAGS_DATA_TYPE_OVERRIDE).value() != null) {
            ret.put(PROPERTY_TAGS_DATA_TYPE_OVERRIDE, ((List<String>) configValues.get(PROPERTY_TAGS_DATA_TYPE_OVERRIDE).value()).stream().collect(Collectors.joining(",")));
        }
        return Collections.singletonList(ret);

    }

    @Override
    public void stop() {
        logger.info("Stopping OPC-DA connector (version {}) on server {}", version(), configValues.get(CommonDefinitions.PROPERTY_SERVER_URI).value());
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }
}
