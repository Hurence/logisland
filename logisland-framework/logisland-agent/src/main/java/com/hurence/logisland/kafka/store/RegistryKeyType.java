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
package com.hurence.logisland.kafka.store;

public enum RegistryKeyType {
    JOB("JOB"),
    TOPIC("TOPIC"),
    NOOP("NOOP");

    public final String keyType;

    private RegistryKeyType(String keyType) {
        this.keyType = keyType;
    }

    public static RegistryKeyType forName(String keyType) {
        if (JOB.keyType.equals(keyType)) {
            return JOB;
        } else if (TOPIC.keyType.equals(keyType)) {
            return TOPIC;
        } else if (NOOP.keyType.equals(keyType)) {
            return NOOP;
        } else {
            throw new IllegalArgumentException("Unknown registry key type : " + keyType
                    + " Valid key types are {config, schema}");
        }
    }
}

