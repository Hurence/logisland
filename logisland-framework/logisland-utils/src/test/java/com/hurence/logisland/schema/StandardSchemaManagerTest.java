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