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
package com.hurence.logisland.record;

/**
 * Set of allowed values for a field type
 * <p>
 * https://avro.apache.org/docs/1.8.1/spec.html#schema_primitive
 */
public enum FieldType {

    NULL,
    STRING,
    INT,
    LONG,
    ARRAY,
    FLOAT,
    DOUBLE,
    BYTES,
    RECORD,
    MAP,
    ENUM,
    BOOLEAN,
    UNION,
    DATETIME;

    public String toString() {
        return name;
    }
    private String name;

    FieldType() {
        this.name = this.name().toLowerCase();
    }

    public String getName() {
        return name;
    }
}
