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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.record;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author tom
 */
public class StandardRecordTest {


    @Test
    public void validateRecordApi() throws IOException {

        String id = "firewall_record1";
        String type = "cisco";
        Record record = new StandardRecord(type).setId(id);

        assertTrue(record.isEmpty());
        assertEquals(record.size(), 0);

        // shortcut for id
        assertEquals(record.getId(), id);
        assertEquals(record.getField(FieldDictionary.RECORD_ID).asString(), id);

        // shortcut for time
        assertEquals(record.getTime().getTime(), record.getField(FieldDictionary.RECORD_TIME).asLong().longValue());

        // shortcut for type
        assertEquals(record.getType(), type);
        assertEquals(record.getType(), record.getField(FieldDictionary.RECORD_TYPE).asString());
        assertEquals(record.getType(), record.getField(FieldDictionary.RECORD_TYPE).getRawValue());


        record.setField("timestamp", FieldType.LONG, new Date().getTime())
                .setField("method", FieldType.STRING, "GET")
                .setField("ip_source", FieldType.STRING, "123.34.45.123")
                .setField("ip_target", FieldType.STRING, "255.255.255.255")
                .setField("url_scheme", FieldType.STRING, "http")
                .setStringField("url_host", "origin-www.20minutes.fr")
                .setField("url_port", FieldType.STRING, "80")
                .setField("url_path", FieldType.STRING, "/r15lgc-100KB.js")
                .setField("request_size", FieldType.INT, 1399)
                .setField("response_size", FieldType.INT, 452)
                .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")))
                .setField("type", FieldType.UNION, new ArrayList<>(Arrays.asList("null", "string")));

        assertEquals(record.size(), 14);  // 14 + 3

        assertFalse(record.hasField("unkown_field"));
        assertTrue(record.hasField("method"));
        assertEquals(record.getField("method").asString(), "GET");
        assertTrue(record.getField("response_size").asInteger() - 452 == 0);

        record.removeField("is_host_blacklisted");
        assertEquals(record.size(), 13);

        record.setField("is_outside_office_hours", record.getField("is_outside_office_hours").getType(), !record.getField("is_outside_office_hours").asBoolean());
        assertTrue(record.getField("is_outside_office_hours").asBoolean());
        assertTrue(record.hasField("type"));
    }


    @Test
    public void validateRecordValidity() {


        Record record = new StandardRecord();
        record.setField("request_size", FieldType.INT, 1399);
        assertTrue(record.isValid());
        record.setField("request_size", FieldType.INT, "zer");
        assertFalse(record.isValid());
        record.setField("request_size", FieldType.INT, 45L);
        assertFalse(record.isValid());
        record.setField("request_size", FieldType.LONG, 45L);
        assertTrue(record.isValid());
        record.setField("request_size", FieldType.DOUBLE, 45.5d);
        assertTrue(record.isValid());
        record.setField("request_size", FieldType.DOUBLE, 45.5);
        assertTrue(record.isValid());
        record.setField("request_size", FieldType.DOUBLE, 45L);
        assertFalse(record.isValid());
        record.setField("request_size", FieldType.FLOAT, 45.5f);
        assertTrue(record.isValid());
        record.setField("request_size", FieldType.STRING, 45L);
        assertFalse(record.isValid());
        record.setField("request_size", FieldType.FLOAT, 45.5d);
        assertFalse(record.isValid());


        record.setField("request_size", FieldType.INT, 45);
        assertTrue(45.0d - record.getField("request_size").asDouble() == 0);
    }


    @Test
    public void validateNestedRecord() {


        Record leafRecord = new StandardRecord("leaf");
        leafRecord.setField("request_size", FieldType.INT, 1399);
        assertTrue(leafRecord.isValid());

        Record rootRecord = new StandardRecord("root");
        rootRecord.setField("request_str", FieldType.STRING, "zer");
        assertTrue(rootRecord.isValid());

        rootRecord.setField("record_leaf", FieldType.RECORD, leafRecord);
        assertTrue(rootRecord.isValid());


        assertTrue(rootRecord.getField("record_leaf").asRecord()
                .getField("request_size").asInteger() == 1399);



    }

    @Test
    public void validatePosition() {

        Record rootRecord = new StandardRecord("root");
        rootRecord.setField("request_str", FieldType.STRING, "zer");
        assertTrue(rootRecord.isValid());


        Position position = Position.from(1.0, 2.0, 3.0, 4.0, 5.0, 6,7, 8.0, new Date(10));



        assertFalse(rootRecord.hasPosition());
        rootRecord.setPosition(position);
        assertTrue(rootRecord.hasPosition());


        assertTrue(rootRecord.getPosition().getAltitude() == 1.0 );
        assertTrue(rootRecord.getPosition().getHeading() == 2.0 );
        assertTrue(rootRecord.getPosition().getLatitude() == 3.0 );
        assertTrue(rootRecord.getPosition().getLongitude() == 4.0 );
        assertTrue(rootRecord.getPosition().getPrecision() == 5.0 );
        assertTrue(rootRecord.getPosition().getSatellites() == 6 );
        assertTrue(rootRecord.getPosition().getStatus() == 7 );
        assertTrue(rootRecord.getPosition().getSpeed() == 8.0 );
        assertTrue(rootRecord.getPosition().getTimestamp().getTime() == 10L );

    }

    /**
     * tests
     *     NULL,
     *     STRING,
     *     INT,
     *     LONG,
     *     FLOAT,
     *     DOUBLE,
     *     BYTES,
     *     BOOLEAN,
     * @throws FieldTypeException
     */
    @Test
    public void testStrongTypeCheckingBasicTypes() throws FieldTypeException {
        final Record record = new StandardRecord();
        //INT
        record.setCheckedField("request_size", FieldType.INT, 1399);
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.INT, 1L);
        });
        //LONG
        record.setCheckedField("request_size", FieldType.LONG, 1399L);
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.LONG, 154);
        });
        //DOUBLE
        record.setCheckedField("request_size", FieldType.DOUBLE, 1399d);
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.DOUBLE, 1L);
        });
        //FLOAT
        record.setCheckedField("request_size", FieldType.FLOAT, 1399f);
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.FLOAT, 145);
        });
        //String
        record.setCheckedField("request_size", FieldType.STRING, "ffff");
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.STRING, 1L);
        });
        //NULL
        record.setCheckedField("request_size", FieldType.NULL, null);
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.NULL, 1L);
        });
        //BYTES
        record.setCheckedField("request_size", FieldType.BYTES, new byte[]{122, -2});
        record.setCheckedField("request_size", FieldType.BYTES, new Byte[]{122, -2});
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.BYTES, 1L);
        });
        //BOOLEAN
        record.setCheckedField("request_size", FieldType.BOOLEAN, true);
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.BOOLEAN, 1L);
        });
    }

    /**
     * tests
     *     ARRAY,
     *     RECORD,
     *     MAP,
     *     ENUM,
     *     UNION,
     *     DATETIME;
     * @throws FieldTypeException
     */
    @Test
    public void testStrongTypeCheckingComplexTypes() throws FieldTypeException {
        final Record record = new StandardRecord();
        //ARRAY
        record.setCheckedField("request_size", FieldType.ARRAY, new String[]{"a", "b"});
        assertTrue(record.isValid());
        record.setCheckedField("request_size", FieldType.ARRAY, new Long[]{1L, 2L});
        assertTrue(record.isValid());
        record.setCheckedField("request_size", FieldType.ARRAY, new Record[]{record, record});
        assertTrue(record.isValid());
        record.setCheckedField("request_size", FieldType.ARRAY, Arrays.asList("a", "b"));
        assertTrue(record.isValid());
        record.setCheckedField("request_size", FieldType.ARRAY, Arrays.asList(1L, 2L));
        assertTrue(record.isValid());
        record.setCheckedField("request_size", FieldType.ARRAY, Arrays.asList(record, record));
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.ARRAY, 1L);
        });
        //RECORD
        record.setCheckedField("request_size", FieldType.RECORD, record);
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.RECORD, 154);
        });
        //MAP
        record.setCheckedField("request_size", FieldType.MAP, new HashMap<>());
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.MAP, 1L);
        });
        //ENUM
        record.setCheckedField("request_size", FieldType.ENUM, FieldType.BOOLEAN);
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.ENUM, record);
        });
        //DATETIME
        record.setCheckedField("request_size", FieldType.DATETIME, new Date());
        assertTrue(record.isValid());
        assertThrows(FieldTypeException.class, () -> {
            record.setCheckedField("request_size", FieldType.DATETIME, 1L);
        });
    }
}