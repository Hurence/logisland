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
package com.hurence.logisland.util.record;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;


public class RecordSchemaUtilTest {
    @Test
    public void convertToValidRecord() throws Exception {


        Record inputRecord = new StandardRecord("apache_log");

        inputRecord.setStringField("src_ip", "10.3.10.134");
        inputRecord.setStringField("http_method", "GET");
        inputRecord.setStringField("bytes_out", "51");
        inputRecord.setStringField("http_query", "/usr/rest/account/email");
        inputRecord.setStringField("http_version", "HTTP/1.1");
        inputRecord.setStringField("identd", "-");
        inputRecord.setStringField("user", "-");
        inputRecord.setStringField("bad_record", "who's bad");


        String strSchema = "{  \"version\":1,\n" +
                "             \"type\": \"record\",\n" +
                "             \"name\": \"com.hurence.logisland.record.apache_log\",\n" +
                "             \"fields\": [\n" +
                "               { \"name\": \"record_error\",  \"type\":  [{\"type\": \"array\", \"items\": \"string\"},\"null\"] },\n" +
                "               { \"name\": \"record_raw_value\",   \"type\": [\"string\",\"null\"] },\n" +
                "               { \"name\": \"record_id\",   \"type\": [\"string\",\"null\"] },\n" +
                "               { \"name\": \"record_time\", \"type\": [\"long\"] },\n" +
                "               { \"name\": \"record_type\", \"type\": [\"string\",\"null\"] },\n" +
                "               { \"name\": \"src_ip\",      \"type\": [\"string\",\"null\"] },\n" +
                "               { \"name\": \"http_method\", \"type\": [\"string\",\"null\"] },\n" +
                "               { \"name\": \"bytes_out\",   \"type\": [\"long\",\"null\"] },\n" +
                "               { \"name\": \"http_query\",  \"type\": [\"string\"] },\n" +
                "               { \"name\": \"http_version\",\"type\": [\"string\",\"null\"] },\n" +
                "               { \"name\": \"http_status\", \"type\": [\"string\",\"null\"] },\n" +
                "               { \"name\": \"identd\", \"type\": [\"string\",\"null\"] },\n" +
                "               { \"name\": \"user\",        \"type\": [\"string\",\"null\"] }    ]}";
        final Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(strSchema);

        Record outputRecord = RecordSchemaUtil.convertToValidRecord(inputRecord, schema);
        Assert.assertTrue(outputRecord.getField("bytes_out").getType() == FieldType.LONG);
        Assert.assertTrue(outputRecord.getField("bytes_out").asLong() == 51);
        Assert.assertTrue(outputRecord.getErrors().isEmpty());
        Assert.assertFalse(outputRecord.hasField("bad_record"));

        inputRecord.removeField("http_query");
        outputRecord = RecordSchemaUtil.convertToValidRecord(inputRecord, schema);
        Assert.assertFalse(outputRecord.getErrors().isEmpty());
    }

}