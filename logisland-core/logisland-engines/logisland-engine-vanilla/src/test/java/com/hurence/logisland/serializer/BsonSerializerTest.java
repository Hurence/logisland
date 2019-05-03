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
package com.hurence.logisland.serializer;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.UUID;

import static org.junit.Assert.*;

public class BsonSerializerTest {

    @Test
    public void serdeTest() {
        BsonSerializer serializer = new BsonSerializer();
        Record testRecord = new StandardRecord();
        testRecord.setTime(new Date());
        testRecord.setId(UUID.randomUUID().toString());
        testRecord.setField(new Field("f2", FieldType.STRING, "Test"));
        testRecord.setField(new Field("f3", FieldType.DOUBLE, 3.1415));
        testRecord.setField(new Field("f4", FieldType.LONG,Long.MAX_VALUE));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(baos, testRecord);
        assertEquals(testRecord, serializer.deserialize(new ByteArrayInputStream(baos.toByteArray())));

    }

}