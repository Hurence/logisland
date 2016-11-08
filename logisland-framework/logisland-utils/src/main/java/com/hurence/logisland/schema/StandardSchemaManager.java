/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.schema;

import com.hurence.logisland.serializer.RecordSerializationException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class StandardSchemaManager implements SchemaManager, Serializable {


    private static Logger logger = LoggerFactory.getLogger(StandardSchemaManager.class);
    Map<String, Schema> schemas = new HashMap<>();


    public Schema addSchema( final String topicName, final String strSchema) {
        final Schema.Parser parser = new Schema.Parser();
        try {
            Schema schema = parser.parse(strSchema);
            schemas.put(topicName, schema);
            return schema;
        } catch (Exception e) {
            logger.error("unable to add schema :{}", e.getMessage());
            return null;
        }
    }

    public Schema addSchema(final String topicName, final InputStream inputStream) {
        assert inputStream != null;
        final Schema.Parser parser = new Schema.Parser();
        try {
            Schema schema = parser.parse(inputStream);
            schemas.put(topicName, schema);
            return schema;
        } catch (IOException e) {
            logger.error("unable to add schema :{}", e.getMessage());
            return null;
        }
    }


    public String getTopicName(final Schema schema) {

        String topic = schema.getProp("topic");
        if (topic == null)
            throw new IllegalArgumentException("no topic name found in metadata ! ");

        return topic;
    }

    @Override
    public Optional<Schema> getTopicSchema(String topicName) {
        if (schemas.containsKey(topicName)) {
            return Optional.of(schemas.get(topicName));
        } else {
            return Optional.empty();
        }
    }
}
