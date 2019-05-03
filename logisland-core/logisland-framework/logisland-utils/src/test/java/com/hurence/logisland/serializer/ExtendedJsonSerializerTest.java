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
package com.hurence.logisland.serializer;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.RecordValidator;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author tom
 */
public class ExtendedJsonSerializerTest {


    @Test
    public void someSchemaTest() throws Exception {
        final String recordStr = "{\n" +
                "  \"firstName\": \"John\",\n" +
                "  \"lastName\" : \"doe\",\n" +
                "  \"age\"      : 26,\n" +
                "  \"address\"  : {\n" +
                "    \"streetAddress\": \"naist street\",\n" +
                "    \"city\"         : \"Nara\",\n" +
                "    \"postalCode\"   : \"630-0192\"\n" +
                "  },\n" +
                "  \"phoneNumbers\": [\n" +
                "    {\n" +
                "      \"type\"  : \"iPhone\",\n" +
                "      \"number\": \"0123-4567-8888\",\n" +
                "      \"value\": \"1.023\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\"  : \"home\",\n" +
                "      \"number\": \"0123-4567-8910\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        final String schema = "{\n" +
                "  \"version\": 1,\n" +
                "  \"type\": \"record\",\n" +
                "  \"namespace\": \"com.hurence.logisland\",\n" +
                "  \"name\": \"Record\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"firstName\",\n" +
                "      \"type\": \"string\"\n" +
                "    }, {\n" +
                "      \"name\": \"lastName\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"age\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"address\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"record\",\n" +
                "        \"name\": \"Address\",\n" +
                "        \"fields\": [\n" +
                "            {\n" +
                "              \"name\": \"streetAddress\",\n" +
                "              \"type\": \"string\"\n" +
                "            }, {\n" +
                "              \"name\": \"city\",\n" +
                "              \"type\": \"string\"\n" +
                "            }\n" +
                "            ]\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"phoneNumbers\",\n" +
                "      \"type\": {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"type\": \"record\",\n" +
                "            \"name\": \"PhoneNumber\",\n" +
                "            \"fields\": [\n" +
                "                {\n" +
                "                  \"name\": \"type\",\n" +
                "                  \"type\": \"string\"\n" +
                "                }, \n" +
                "                {\n" +
                "                  \"name\": \"value\",\n" +
                "                  \"type\": \"double\"\n" +
                "                }\n" +
                "            ]\n" +
                "          }\n" +
                "        \n" +
                "      }\n" +
                "    }\n" +
                "    \n" +
                "  ]\n" +
                "}";


        final ExtendedJsonSerializer serializer = new ExtendedJsonSerializer(schema);
        ByteArrayInputStream bais = new ByteArrayInputStream(recordStr.getBytes());
        Record deserializedRecord = serializer.deserialize(bais);
        System.out.println(deserializedRecord);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(baos, deserializedRecord);
        baos.close();

        final String desRecordStr = new String(baos.toByteArray());
        System.out.println(desRecordStr);

    }


    @Test
    public void validateArrays() throws IOException {

        final ExtendedJsonSerializer serializer = new ExtendedJsonSerializer();
        Map obj1 = new HashMap<>();
        obj1.put("name", "greg");
        obj1.put("numero", 1);
        Map obj2 = new HashMap<>();
        obj2.put("name", "greg2");
        obj2.put("numero", 2);

        Record record = new StandardRecord("cisco");
        record.setId("firewall_record1");
        record.addError("fatal_error", "ouille");
        record.setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")));
        record.setField("numbers", FieldType.ARRAY, new ArrayList<>(Arrays.asList(1, 2, 3)));
        record.setField("object", FieldType.ARRAY, new ArrayList<>(Arrays.asList(obj1, obj2)));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(baos, record);
        baos.close();


        String strEvent = new String(baos.toByteArray());
        System.out.println(strEvent);


        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        Record deserializedRecord = serializer.deserialize(bais);

        assertEquals(record, deserializedRecord);

    }

    @Test
    public void validateJsonSerialization() throws IOException {

        final ExtendedJsonSerializer serializer = new ExtendedJsonSerializer();


        Record record = new StandardRecord("cisco");
        record.setId("firewall_record1");
        record.setField("timestamp", FieldType.LONG, new Date().getTime());
        record.setField("method", FieldType.STRING, "GET");
        record.setField("ip_source", FieldType.STRING, "123.34.45.123");
        record.setField("ip_target", FieldType.STRING, "255.255.255.255");
        record.setField("url_scheme", FieldType.STRING, "http");
        record.setField("url_host", FieldType.STRING, "origin-www.20minutes.fr");
        record.setField("url_port", FieldType.STRING, "80");
        record.setField("url_path", FieldType.STRING, "/r15lgc-100KB.js");
        record.setField("request_size", FieldType.INT, 1399);
        record.setField("response_size", FieldType.INT, 452);
        record.setField("is_outside_office_hours", FieldType.BOOLEAN, false);
        record.setField("is_host_blacklisted", FieldType.BOOLEAN, false);
        //record.setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")));


        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(baos, record);
        baos.close();


        String strEvent = new String(baos.toByteArray());
        System.out.println(strEvent);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        Record deserializedRecord = serializer.deserialize(bais);

        assertEquals(record, deserializedRecord);

    }


    @Test
    public void issueWithDate() {
        final String recordStr = "{ \"id\" : \"0:vfBHTdrZCcFs3H6aO7Yb4UXWVppa80JiKQ7aW0\", \"type\" : \"pageView\", \"creationDate\" : \"Thu Apr 27 11:45:00 CEST 2017\",  \"referer\" : \"https://orexad.preprod.group-iph.com/fr/equipement/c-45\",   \"B2BUnit\" : null,   \"eventCategory\" : null,   \"remoteHost\" : \"149.202.66.102\",   \"userAgentString\" : \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\",   \"eventAction\" : null,   \"categoryName\" : null,   \"viewportPixelWidth\" : 1624,   \"hitType\" : null,   \"companyID\" : null,   \"pageType\" : null,   \"Userid\" : null,   \"localPath\" : null,   \"partyId\" : \"0:j1ymvypx:pO~iVklsXUYa6zMKVZeA2nC1YlVzTEw1\",   \"codeProduct\" : null,   \"is_newSession\" : false,   \"GroupCode\" : null,     \"sessionId\" : \"0:j20802wr:Py1qDrBry7UedH6my~6ebE58wRHUXWVp\",   \"categoryCode\" : null,   \"eventLabel\" :null,   \"record_type\" : \"pageView\",   \"n\" : null,   \"record_id\" : \"0:vfBHTdrZCcFs3H6aO7Yb4pa80JiKQ7aW0\",   \"q\" : null,   \"userRoles\" : null,   \"screenPixelWidth\" : 1855,   \"viewportPixelHeight\" : 726,   \"screenPixelHeight\" : 1056,   \"is_PunchOut\" : null,   \"h2kTimestamp\" : 1493286295730,  \"pageViewId\" : \"0:vfBHTdrZCcFs3H6aO7Yb4pa80JiKQ7aW\",   \"location\" : \"https://orexad.preprod.group-iph.com/fr/equipement/c-45\",   \"record_time\" : 1493286300033,   \"pointOfService\" : null }";

        final ExtendedJsonSerializer serializer = new ExtendedJsonSerializer();
        ByteArrayInputStream bais = new ByteArrayInputStream(recordStr.getBytes());
        Record deserializedRecord = serializer.deserialize(bais);
        assertTrue(deserializedRecord.getTime().getTime() == 1493286300033L);
    }

    @Test
    public void testFormatDate() throws Exception {
        final String schema = "{\n" +
                "  \"version\": 1,\n" +
                "  \"type\": \"record\",\n" +
                "  \"namespace\": \"test\",\n" +
                "  \"name\": \"test\",\n" +
                "  \"fields\": [  \n" +
                "    {\n" +
                "      \"name\": \"myDateAsString\",\n" +
                "      \"type\":  {\n" +
                "        \"type\": \"int\",\n" +
                "        \"logicalType\": \"date\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"myTimestampAsString\",\n" +
                "      \"type\":  {\n" +
                "        \"type\": \"long\",\n" +
                "        \"logicalType\": \"timestamp-millis\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"myTimestampAsEpoch\",\n" +
                "      \"type\":  {\n" +
                "        \"type\": \"long\",\n" +
                "        \"logicalType\": \"timestamp-millis\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"unformattedDate\",\n" +
                "      \"type\":  \"long\"\n" +
                "    }\n" +
                " ]\n" +
                "}\n";

        final String json = "{\n" +
                "  \"myTimestampAsEpoch\": 1541494638000,\n" +
                "  \"myTimestampAsString\" : \"2018-11-06T08:57:18.000Z\",\n" +
                "  \"myDateAsString\": \"20181106\",\n" +
                "  \"unformattedDate\" : 1541494638000\n" +
                "}";


        final ExtendedJsonSerializer serializer = new ExtendedJsonSerializer(schema);
        final Record record = serializer.deserialize(new ByteArrayInputStream(json.getBytes("UTF8")));
        System.out.println(record);
        assertEquals(FieldType.DATETIME, record.getField("myTimestampAsEpoch").getType());
        assertEquals(FieldType.DATETIME, record.getField("myTimestampAsString").getType());
        assertEquals(FieldType.DATETIME, record.getField("myDateAsString").getType());
        assertEquals(FieldType.LONG, record.getField("unformattedDate").getType());

    }

    @Test
    public void deserialzeWithObject() {
        //expected record
        Map<String, Object> deviceMap = new HashMap<>();
        deviceMap.put("category", "mobile");
        deviceMap.put("mobile_brand_name", null);
        deviceMap.put("mobile_os_hardware_model", "x86_64");
        deviceMap.put("time_zone_offset_seconds", 7200);
        final Record expectedRecord = new StandardRecord();
        expectedRecord.setField("device", FieldType.MAP, deviceMap);

        final ExtendedJsonSerializer serializer = new ExtendedJsonSerializer();
        //record from rawJson Deserialization
        final String recordStr = "{\"device\":{\"category\":\"mobile\",\"mobile_brand_name\":null,\"mobile_os_hardware_model\":\"x86_64\",\"time_zone_offset_seconds\":7200}}";
        ByteArrayInputStream baisFromRawJson = new ByteArrayInputStream(recordStr.getBytes());
        Record deserializedRecord = serializer.deserialize(baisFromRawJson);

        expectedRecord.setId(deserializedRecord.getId());
        expectedRecord.setTime(deserializedRecord.getTime());
        expectedRecord.setType(deserializedRecord.getType());
        //tests
        assertEquals(expectedRecord, deserializedRecord);

    }

    @Test
    public void deserialzeWithArrays() {
        //json
        final String recordStr = "{\"event_params\":[" +
                "{\"key\":\"category\",\"value\":{\"string_value\":\"api\",\"int_value\":null,\"float_value\":null,\"double_value\":null}}," +
                "{\"key\":\"firebase_event_origin\",\"value\":{\"string_value\":\"app\",\"int_value\":null,\"float_value\":null,\"double_value\":null}}" +
        "]}";
        //expected record
        ArrayList<Map<String, Object>> eventparams = new ArrayList<>();
        Map<String, Object> param1 = new HashMap<>();
        param1.put("key", "category");
        Map<String, Object> valueParam1 = new HashMap<>();
        valueParam1.put("string_value", "api");
        valueParam1.put("int_value", null);
        valueParam1.put("float_value", null);
        valueParam1.put("double_value", null);
        param1.put("value", valueParam1);
        Map<String, Object> param2 = new HashMap<>();
        param2.put("key", "firebase_event_origin");
        Map<String, Object> valueParam2 = new HashMap<>();
        valueParam2.put("string_value", "app");
        valueParam2.put("int_value", null);
        valueParam2.put("float_value", null);
        valueParam2.put("double_value", null);
        param2.put("value", valueParam2);
        eventparams.add(param1);
        eventparams.add(param2);

        final Record expectedRecord = new StandardRecord();
        expectedRecord.setField("event_params", FieldType.ARRAY, eventparams);

        final ExtendedJsonSerializer serializer = new ExtendedJsonSerializer();
        ByteArrayInputStream bais = new ByteArrayInputStream(recordStr.getBytes());
        Record deserializedRecord = serializer.deserialize(bais);

        expectedRecord.setId(deserializedRecord.getId());
        expectedRecord.setTime(deserializedRecord.getTime());
        expectedRecord.setType(deserializedRecord.getType());

        assertEquals(expectedRecord, deserializedRecord);
    }

    @Test
    public void deserialzeIdToRecordId() {
        //json
        final String recordStr = "{\"id\":\"id_record\", \"name\": \"greg\"}";
        //expected record
        ArrayList<Map<String, Object>> eventparams = new ArrayList<>();

        final Record expectedRecord = new StandardRecord();
        expectedRecord.setId("id_record");
        expectedRecord.setField("name", FieldType.STRING, "greg");

        final ExtendedJsonSerializer serializer = new ExtendedJsonSerializer();
        ByteArrayInputStream bais = new ByteArrayInputStream(recordStr.getBytes());
        Record deserializedRecord = serializer.deserialize(bais);

        expectedRecord.setTime(deserializedRecord.getTime());
        expectedRecord.setType(deserializedRecord.getType());

        assertEquals(expectedRecord, deserializedRecord);
    }
}