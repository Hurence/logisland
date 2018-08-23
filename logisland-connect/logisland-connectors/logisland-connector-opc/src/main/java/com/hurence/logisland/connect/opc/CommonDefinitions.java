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

package com.hurence.logisland.connect.opc;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.Duration;

public class CommonDefinitions {

    public static final String PROPERTY_TAGS_ID = "tags.id";
    public static final String PROPERTY_TAGS_SAMPLING_RATE = "tags.sampling.rate";
    public static final String PROPERTY_TAGS_STREAM_MODE = "tags.stream.mode";
    public static final String PROPERTY_CONNECTION_SOCKET_TIMEOUT = "connection.socketTimeoutMillis";
    public static final String PROPERTY_SERVER_URI = "server.uri";

    /**
     * Add the default configuration (common for UA and DA).
     *
     * @param configDef the config definition.
     * @return the config definition that was in input.
     */
    public static final ConfigDef addCommonProperties(ConfigDef configDef) {
        return configDef.define(PROPERTY_SERVER_URI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                "The OPC-DA server uri. (e.g. opc.da://host:port)")
                .define(PROPERTY_TAGS_ID, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                        "Comma separated tags id to subscribe to")
                .define(PROPERTY_TAGS_SAMPLING_RATE, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                        "Comma separated tags sampling rate as ISO 8601 durations(e.g. PT8H6M12.345S")
                .define(PROPERTY_TAGS_STREAM_MODE, ConfigDef.Type.LIST, ConfigDef.Importance.MEDIUM,
                        "Comma separated tags streaming mode. Each value can be either POLL or SUBSCRIBE")
                .define(PROPERTY_CONNECTION_SOCKET_TIMEOUT, ConfigDef.Type.LONG, 10_000, ConfigDef.Importance.LOW,
                        "The socket timeout (defaults to 10 seconds)");
    }


}
