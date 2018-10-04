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
package com.hurence.logisland.schema;

import com.hurence.logisland.serializer.RecordSerializationException;
import org.apache.avro.Schema;

import java.io.InputStream;
import java.util.Optional;

public interface SchemaManager {

    /**
     * retrieve the avro schema associated to a Topic
     *
     * @param topicName the name of the topic
     * @return the associated schema
     */
    Optional<Schema> getTopicSchema(final String topicName);

    Schema addSchema(final String topicName, final String strSchema);

    Schema addSchema(final String topicName, final InputStream inputStream);


}
