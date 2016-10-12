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
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author tom
 */
public class StandardRecordTest {


    @Test
    public void validateRecordApi() throws IOException {

        String id = "firewall_record1";
        String type = "cisco";
        Record record = new StandardRecord(type);
        record.setId(id);

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


        record.setField("timestamp", FieldType.LONG, new Date().getTime());
        record.setField("method", FieldType.STRING, "GET");
        record.setField("ip_source", FieldType.STRING, "123.34.45.123");
        record.setField("ip_target", FieldType.STRING, "255.255.255.255");
        record.setField("url_scheme", FieldType.STRING, "http");
        record.setStringField("url_host", "origin-www.20minutes.fr");
        record.setField("url_port", FieldType.STRING, "80");
        record.setField("url_path", FieldType.STRING, "/r15lgc-100KB.js");
        record.setField("request_size", FieldType.INT, 1399);
        record.setField("response_size", FieldType.INT, 452);
        record.setField("is_outside_office_hours", FieldType.BOOLEAN, false);
        record.setField("is_host_blacklisted", FieldType.BOOLEAN, false);
        record.setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")));

        assertEquals(record.size(), 13);  // 13 + 3

        assertFalse(record.hasField("unkown_field"));
        assertTrue(record.hasField("method"));
        assertEquals(record.getField("method").asString(), "GET");
        assertTrue(record.getField("response_size").asInteger() - 452 == 0);

        record.removeField("is_host_blacklisted");
        assertEquals(record.size(), 12);

        record.setField("is_outside_office_hours", record.getField("is_outside_office_hours").getType(), !record.getField("is_outside_office_hours").asBoolean());
        assertTrue(record.getField("is_outside_office_hours").asBoolean());
    }


    @Test(expected = ClassCastException.class)
    public void validateRecordTypeConvertion() throws IOException {

        List<String> tags = new ArrayList<>(Arrays.asList("spam", "filter", "mail"));
        Record record = new StandardRecord();
        record.setField("request_size", FieldType.INT, 1399);
        Field field = record.getField("request_size");
        field.asFloat();
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
    }
}