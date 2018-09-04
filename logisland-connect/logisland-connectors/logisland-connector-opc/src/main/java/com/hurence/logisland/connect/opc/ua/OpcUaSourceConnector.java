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
package com.hurence.logisland.connect.opc.ua;

import com.hurence.logisland.connect.opc.CommonDefinitions;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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

    public static final String PROPERTY_AUTH_BASIC_USER = "auth.basic.user";
    public static final String PROPERTY_AUTH_BASIC_PASSWORD = "auth.basic.password";
    public static final String PROPERTY_AUTH_X509_CERTIFICATE = "auth.x509.certificate";
    public static final String PROPERTY_AUTH_X509_PRIVATE_KEY = "auth.x509.key";
    public static final String PROPERTY_CHANNEL_CERTIFICATE = "connection.certificate";
    public static final String PROPERTY_CHANNEL_PRIVATE_KEY = "connection.key";
    public static final String PROPERTY_CLIENT_URI = "connection.client.uri";
    public static final String PROPERTY_DATA_PUBLICATION_RATE = "session.publicationRate";


    /**
     * The configuration.
     */
    private static final ConfigDef CONFIG = CommonDefinitions.addCommonProperties(new ConfigDef())
            .define(PROPERTY_AUTH_BASIC_USER, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "(User/Password security): The login user")
            .define(PROPERTY_AUTH_BASIC_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "(User/Password security): the login password")
            .define(PROPERTY_AUTH_X509_CERTIFICATE, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "(X509 security): The certificate")
            .define(PROPERTY_AUTH_X509_PRIVATE_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "(X509 security): The private key")
            .define(PROPERTY_CLIENT_URI, ConfigDef.Type.STRING, "urn:hurence:logisland", ConfigDef.Importance.MEDIUM, "The client URI (defaults to urn:hurence:logisland)")
            .define(PROPERTY_CHANNEL_CERTIFICATE, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "In case the connection is secure, the client will have a certificate")
            .define(PROPERTY_CHANNEL_PRIVATE_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "In case the connection is secure, the client will have a private key")
            .define(PROPERTY_DATA_PUBLICATION_RATE, ConfigDef.Type.STRING, Duration.ofSeconds(1).toString(),
                    (name, value) -> {
                        try {
                            Duration.parse((String) value);
                        } catch (Exception e) {
                            throw new ConfigException("Property " + name + " is not a valid duration");
                        }
                    }, ConfigDef.Importance.LOW, "The data publication window as ISO 8601 Duration");


    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        //shallow copy
        configValues = config().validate(props).stream().collect(Collectors.toMap(ConfigValue::name, Function.identity()));
        logger.info("Starting OPC-UA connector (version {}) on server {} reading tags {}", version(),
                configValues.get(CommonDefinitions.PROPERTY_SERVER_URI).value(), configValues.get(CommonDefinitions.PROPERTY_TAGS_ID).value());
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
        ret.put(CommonDefinitions.PROPERTY_TAGS_ID, ((List<String>) configValues.get(CommonDefinitions.PROPERTY_TAGS_ID).value()).stream().collect(Collectors.joining(",")));
        ret.put(CommonDefinitions.PROPERTY_TAGS_STREAM_MODE, ((List<String>) configValues.get(CommonDefinitions.PROPERTY_TAGS_STREAM_MODE).value()).stream().collect(Collectors.joining(",")));
        ret.put(CommonDefinitions.PROPERTY_TAGS_SAMPLING_RATE, ((List<String>) configValues.get(CommonDefinitions.PROPERTY_TAGS_SAMPLING_RATE).value()).stream().collect(Collectors.joining(",")));

        return Collections.singletonList(ret);
    }

    @Override
    public void stop() {
        logger.info("Stopping OPC-UA connector (version {}) on server {}", version(), configValues.get(CommonDefinitions.PROPERTY_SERVER_URI).value());
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }
}
