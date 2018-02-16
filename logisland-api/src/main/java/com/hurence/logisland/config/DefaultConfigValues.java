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
package com.hurence.logisland.config;

/**
 * Set of default values for config
 */
public enum DefaultConfigValues {

    ES_HOSTS ("sandbox:9200"),
    KAFKA_BROKERS ("sandbox:9092"),
    ZK_QUORUM ("sandbox:2181"),
    SOLR_HOSTS ("sandbox:8983"),
    LOGISLAND_AGENT_HOST("sandbox:8008");

    @Override
    public String toString() {
        return "DefaultConfigValues{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    private final String name;
    private final String value;

    DefaultConfigValues(String value) {
        this.name = this.name();
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }
}
