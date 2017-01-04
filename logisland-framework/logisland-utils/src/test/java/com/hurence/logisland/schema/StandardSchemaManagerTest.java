/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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

import com.hurence.logisland.serializer.AvroSerializerTest;
import org.apache.avro.Schema;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;


public class StandardSchemaManagerTest {

    static final String EVENT_SCHEMA = "/schemas/event.avsc";

    @Test
    public void getTopicSchema() throws Exception {

        SchemaManager mgr = new StandardSchemaManager();

        mgr.addSchema("logisland_events", StandardSchemaManagerTest.class.getResourceAsStream(EVENT_SCHEMA));

        assertTrue(mgr.getTopicSchema("logisland_events").isPresent());
        assertFalse(mgr.getTopicSchema("logisland_events2").isPresent());
    }

}