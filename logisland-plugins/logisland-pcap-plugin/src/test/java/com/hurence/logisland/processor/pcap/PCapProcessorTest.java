/**
 * Copyright (C) 2017 Hurence 
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
package com.hurence.logisland.processor.pcap;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;

import static com.hurence.logisland.processor.pcap.PCapConstants.Fields.*;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

//import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

/**
 * Test PCap processor.
 */
public class PCapProcessorTest {
    
    private static Logger logger = LoggerFactory.getLogger(PCapProcessorTest.class);

    
    @Test
    public void testSmallPCapRecordProcessing() {
        final TestRunner testRunner = TestRunners.newTestRunner(new PCapProcessor());
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        Path pcapFile = Paths.get("./src/test/resources/verySmallFlows.pcap");
        try {
            byte[] pcapbytes = Files.readAllBytes(pcapFile);
            record.setField(FieldDictionary.RECORD_KEY, FieldType.LONG, 1338882754996790000L);
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

        } catch (IOException e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(4);

        MockRecord out = testRunner.getOutputRecords().get(0);
        
        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "network packet");
        
        out.assertFieldExists(TIMESTAMP.getName());
        out.assertFieldEquals(TIMESTAMP.getName(), 1338882754996790000L);

        out.assertFieldExists(PROTOCOL.getName());
        out.assertFieldEquals(PROTOCOL.getName(), "6");

        out.assertFieldExists(SRC_ADDR.getName());
        out.assertFieldEquals(SRC_ADDR.getName(), "192.168.10.226");
        
        out.assertFieldExists(SRC_PORT.getName());
        out.assertFieldEquals(SRC_PORT.getName(), (int)19707);
        
        out.assertFieldExists(DST_ADDR.getName());
        out.assertFieldEquals(DST_ADDR.getName(), "192.168.11.12");
        
        out.assertFieldExists(DST_PORT.getName());
        out.assertFieldEquals(DST_PORT.getName(), (int)23);

    }

    @Test
    public void testMediumPCapRecordProcessing() {
        final TestRunner testRunner = TestRunners.newTestRunner(new PCapProcessor());
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        Path pcapFile = Paths.get("./src/test/resources/mediumFlows.pcap");
        try {
            byte[] pcapbytes = Files.readAllBytes(pcapFile);
            record.setField(FieldDictionary.RECORD_KEY, FieldType.LONG, 1338882754996790000L);
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

        } catch (IOException e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(14261);


    }
}
