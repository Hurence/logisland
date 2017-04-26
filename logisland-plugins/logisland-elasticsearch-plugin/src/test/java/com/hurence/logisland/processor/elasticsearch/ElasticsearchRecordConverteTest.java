/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.processor.elasticsearch;


import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.elasticsearch.ElasticsearchRecordConverter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;


public class ElasticsearchRecordConverteTest {


    private static Logger logger = LoggerFactory.getLogger(ElasticsearchRecordConverteTest.class);


    @Test
    public void validate() throws Exception {

        final String indexName = "test";
        final String recordType = "cisco_record";


        Record[] records = {
                new StandardRecord(recordType)
                        .setId("firewall_record1")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "GET")
                        .setField("ip_source", FieldType.STRING, "123.34.45.123")
                        .setField("ip_target", FieldType.STRING, "255.255.255.255")
                        .setField("url_scheme", FieldType.STRING, "http")
                        .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                        .setField("url_port", FieldType.STRING, "80")
                        .setField("url_path", FieldType.STRING, "/r15lgc-100KB.js")
                        .setField("request_size", FieldType.INT, 1399)
                        .setField("response_size", FieldType.INT, 452)
                        .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                        .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail"))),
                new StandardRecord(recordType)
                        .setId("firewall_record1")
                        .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, 1475525688668L)
                        .setField("method", FieldType.STRING, "GET")
                        .setField("ip_source", FieldType.STRING, "123.34.45.12")
                        .setField("ip_target", FieldType.STRING, "255.255.255.255")
                        .setField("url_scheme", FieldType.STRING, "http")
                        .setField("url_host", FieldType.STRING, "origin-www.20minutes.fr")
                        .setField("url_port", FieldType.STRING, "80")
                        .setField("url_path", FieldType.STRING, 45)
                        .setField("request_size", FieldType.INT, 1399)
                        .setField("response_size", FieldType.INT, 452)
                        .setField("is_outside_office_hours", FieldType.BOOLEAN, false)
                        .setField("is_host_blacklisted", FieldType.BOOLEAN, false)
                        .setField("tags", FieldType.ARRAY, new ArrayList<>(Arrays.asList("spam", "filter", "mail")))
        };


        String converterRecord0 = ElasticsearchRecordConverter.convert(records[0]);
    }
}