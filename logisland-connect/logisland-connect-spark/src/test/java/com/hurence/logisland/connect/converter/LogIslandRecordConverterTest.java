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

package com.hurence.logisland.connect.converter;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.BytesArraySerializer;
import com.hurence.logisland.serializer.KryoSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.SerializerProvider;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.*;

import static org.junit.Assert.*;

public class LogIslandRecordConverterTest {

    private LogIslandRecordConverter setupInstance(Class<? extends RecordSerializer> serializerClass, boolean isKey) {
        final LogIslandRecordConverter instance = new LogIslandRecordConverter();
        instance.configure(
                Collections.singletonMap(LogIslandRecordConverter.PROPERTY_RECORD_SERIALIZER, serializerClass.getCanonicalName()),
                isKey);
        return instance;
    }

    private void assertFieldEquals(Record record, String fieldName, Object expected) {
        Field field = record.getField(fieldName);
        assertNotNull(field);
        assertEquals(expected, record.getField(fieldName).getRawValue());
    }

    private void assertFieldEquals(Record record, String fieldName, byte[] expected) {
        Field field = record.getField(fieldName);
        assertNotNull(field);
        assertArrayEquals(expected, (byte[]) record.getField(fieldName).getRawValue());
    }


    @Test
    public void testBytesSchema() {
        byte[] data = new byte[16];
        new Random().nextBytes(data);
        RecordSerializer serializer = new BytesArraySerializer();
        LogIslandRecordConverter instance = setupInstance(serializer.getClass(), false);
        byte[] serialized = instance.fromConnectData("", Schema.BYTES_SCHEMA, data);
        Record record = serializer.deserialize(new ByteArrayInputStream(serialized));
        assertNotNull(record);
        assertFieldEquals(record, FieldDictionary.RECORD_VALUE, data);
    }

    @Test
    public void testComplexSchema() {
        //our schema

        final Schema complexSchema = SchemaBuilder
                .struct()
                .field("f1", SchemaBuilder.bool())
                .field("f2", SchemaBuilder.string())
                .field("f3", SchemaBuilder.int8())
                .field("f4", SchemaBuilder.int16())
                .field("f5", SchemaBuilder.string().optional())
                .field("f6", SchemaBuilder.float32())
                .field("arr", SchemaBuilder.array(SchemaBuilder.int32()))
                .field("map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()))
                .field("struct", SchemaBuilder.struct()
                        .field("child", SchemaBuilder.string()).build())
                .build();

        //setup converters
        LogIslandRecordConverter instance = setupInstance(KryoSerializer.class, false);
        RecordSerializer serializer = SerializerProvider.getSerializer(KryoSerializer.class.getName(), null);
        Struct complex = new Struct(complexSchema)
                .put("f1", true)
                .put("f2", "test")
                .put("f3", (byte) 0)
                .put("f4", (short) 1)
                .put("f5", null)
                .put("f6", 3.1415f)
                .put("arr", new ArrayList<>(Arrays.asList(0, 1, 2)))
                .put("map", new HashMap<>(Collections.singletonMap("key", "value")))
                .put("struct",
                        new Struct(complexSchema.field("struct").schema())
                                .put("child", "child"));

        Record record = serializer.deserialize(new ByteArrayInputStream(instance.fromConnectData(null, complexSchema, complex)));
        System.out.println(record);
        //assertions
        assertNotNull(record);
        Record extracted = record.getField(FieldDictionary.RECORD_VALUE).asRecord();
        assertNotNull(extracted);
        assertFieldEquals(extracted, "f1", true);
        assertFieldEquals(extracted, "f2", "test");
        assertFieldEquals(extracted, "f3", (byte) 0);
        assertFieldEquals(extracted, "f4", (short) 1);
        assertFieldEquals(extracted, "f5", null);
        assertFieldEquals(extracted, "f6", (float) 3.1415);
        assertFieldEquals(extracted, "arr", new ArrayList<>(Arrays.asList(0, 1, 2)));
        assertFieldEquals(extracted, "map", new HashMap<>(Collections.singletonMap("key", "value")));
        assertFieldEquals(extracted.getField("struct").asRecord(), "child", "child");

    }
}
