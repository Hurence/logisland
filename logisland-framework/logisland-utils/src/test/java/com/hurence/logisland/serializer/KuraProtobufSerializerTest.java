/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.serializer;

import com.hurence.logisland.record.*;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author tom
 */
public class KuraProtobufSerializerTest {


    @Test
    public void kuraValidation() throws IOException {
        final KuraProtobufSerializer serializer = new KuraProtobufSerializer();

        Record record = new StandardRecord(KuraProtobufSerializer.KURA_RECORD);

        Position position = Position.from(1.0, 2.0, 3.0, 4.0, 5.0, 6, 7, 8.0, new Date(10));


        record.setPosition(position);
        record.setTime(12L);


        Record metricA =  new StandardRecord(KuraProtobufSerializer.KURA_METRIC);
        metricA.setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 23.0);
        record.setField("metricA", FieldType.RECORD, metricA);

        Record metricB =  new StandardRecord(KuraProtobufSerializer.KURA_METRIC);
        metricB.setField(FieldDictionary.RECORD_VALUE, FieldType.INT, 21);
        record.setField("metricB", FieldType.RECORD, metricB);

        Record metricC =  new StandardRecord(KuraProtobufSerializer.KURA_METRIC);
        metricC.setField(FieldDictionary.RECORD_VALUE, FieldType.STRING, "oups");
        record.setField("metricC", FieldType.RECORD, metricC);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(baos, record);
        baos.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        Record deserializedRecord = serializer.deserialize(bais);


        assertTrue(deserializedRecord.getTime().getTime() == 12L);
        assertTrue(deserializedRecord.hasPosition());
        assertTrue(deserializedRecord.getPosition().getTimestamp().getTime() == 10L);
        assertTrue(deserializedRecord.getPosition().getLongitude() == 4.0);
        assertTrue(deserializedRecord.getPosition().getLatitude() == 3.0);
        assertTrue(deserializedRecord.getPosition().getAltitude() == 1.0);

        assertTrue(deserializedRecord.getField("metricA").asRecord()
                .getField(FieldDictionary.RECORD_VALUE).asDouble() == 23.0);
        assertTrue(deserializedRecord.getField("metricB").asRecord()
                .getField(FieldDictionary.RECORD_VALUE).asInteger() == 21);
        assertTrue(deserializedRecord.getField("metricC").asRecord()
                .getField(FieldDictionary.RECORD_VALUE).asString() .equals( "oups"));
    }

}