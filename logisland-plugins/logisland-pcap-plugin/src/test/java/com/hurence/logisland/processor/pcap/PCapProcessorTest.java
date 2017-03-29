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

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;

import com.hurence.logisland.agent.rest.api.impl.ProcessorsApiServiceImpl;

import static com.hurence.logisland.processor.pcap.PCapConstants.Fields.*;

//import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test PCap processor.
 */
public class PCapProcessorTest {
    
    private static Logger logger = LoggerFactory.getLogger(PCapProcessorTest.class);

    private void testSampleRecord(MockRecord out) {
        /* Standard Fields : */
        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "network_packet");

        out.assertFieldExists(FieldDictionary.PROCESSOR_NAME);
        out.assertFieldEquals(FieldDictionary.PROCESSOR_NAME, "ParsePCap");

        //////////////////////////
        // Global Header Fields //
        //////////////////////////

        out.assertFieldExists(PCKT_TIMESTAMP_IN_NANOS.getName());
        out.assertFieldEquals(PCKT_TIMESTAMP_IN_NANOS.getName(), 1338882754996790000L);

        //////////////////////
        // IP Header Fields //
        //////////////////////

        /* IP Header's 1st 32-bits word : */
        out.assertFieldExists(IP_VERSION.getName());
        out.assertFieldEquals(IP_VERSION.getName(), 4);

        out.assertFieldExists(IP_INTERNETHEADERLENGTH.getName());
        out.assertFieldEquals(IP_INTERNETHEADERLENGTH.getName(), 20);

        out.assertFieldExists(IP_TYPEOFSERVICE.getName());
        out.assertFieldEquals(IP_TYPEOFSERVICE.getName(), 0);

        out.assertFieldExists(IP_DATAGRAMTOTALLENGTH.getName());
        out.assertFieldEquals(IP_DATAGRAMTOTALLENGTH.getName(), 40);

        /* IP Header's 2nd 32-bits word : */
        out.assertFieldExists(IP_IDENTIFICATION.getName());
        out.assertFieldEquals(IP_IDENTIFICATION.getName(), 19110);

        out.assertFieldExists(IP_FLAGS.getName());
        out.assertFieldEquals(IP_FLAGS.getName(), 2);

        out.assertFieldExists(IP_FRAGMENTOFFSET.getName());
        out.assertFieldEquals(IP_FRAGMENTOFFSET.getName(), 0);

        /* IP Header's 3rd 32-bits word : */
        out.assertFieldExists(IP_TIMETOLIVE.getName());
        out.assertFieldEquals(IP_TIMETOLIVE.getName(), 64);

        out.assertFieldExists(IP_PROTOCOL.getName());
        out.assertFieldEquals(IP_PROTOCOL.getName(), 6);

        out.assertFieldExists(IP_CHECKSUM.getName());
        out.assertFieldEquals(IP_CHECKSUM.getName(), 22763);

        /* IP Header's 4th 32-bits word : */
        out.assertFieldExists(IP_SRCIPADDRESS.getName());
        out.assertFieldEquals(IP_SRCIPADDRESS.getName(), "192.168.10.226");

        /* IP Header's 5th 32-bits word : */
        out.assertFieldExists(IP_DSTIPADDRESS.getName());
        out.assertFieldEquals(IP_DSTIPADDRESS.getName(), "192.168.11.12");

        /* IP Headers's following 32-bits word(s) : */
        out.assertFieldNotExists(IP_OPTIONS.getName());

        out.assertFieldNotExists(IP_PADDING.getName());

        ///////////////////////
        // TCP Header Fields //
        ///////////////////////

        /* TCP Header's 1st 32-bits word : */
        out.assertFieldExists(TCP_SRCPORT.getName());
        out.assertFieldEquals(TCP_SRCPORT.getName(), 19707);

        out.assertFieldExists(TCP_DSTPORT.getName());
        out.assertFieldEquals(TCP_DSTPORT.getName(), 23);

        /* TCP Header's 2nd 32-bits word : */
        out.assertFieldExists(TCP_SEQUENCENUMBER.getName());
        out.assertFieldEquals(TCP_SEQUENCENUMBER.getName(), -406128552);

        /* TCP Header's 3rd 32-bits word : */
        out.assertFieldExists(TCP_ACKNOWLEDGMENTNUMBER.getName());
        out.assertFieldEquals(TCP_ACKNOWLEDGMENTNUMBER.getName(), 638797278);

        /* TCP Header's 4th 32-bits word : */
        out.assertFieldExists(TCP_DATAOFFSET.getName());
        out.assertFieldEquals(TCP_DATAOFFSET.getName(), 5);

        out.assertFieldExists(TCP_FLAGS.getName());
        out.assertFieldEquals(TCP_FLAGS.getName(), 17);

        out.assertFieldExists(TCP_WINDOWSIZE.getName());
        out.assertFieldEquals(TCP_WINDOWSIZE.getName(), 16583);

        /* TCP Header's 5th 32-bits word : */
        out.assertFieldExists(TCP_CHECKSUM.getName());
        out.assertFieldEquals(TCP_CHECKSUM.getName(), 16038);

        out.assertFieldExists(TCP_URGENTPOINTER.getName());
        out.assertFieldEquals(TCP_URGENTPOINTER.getName(), 0);

        /* TCP Headers's following 32-bits word(s) : */
        out.assertFieldNotExists(TCP_OPTIONS.getName());

        out.assertFieldNotExists(TCP_PADDING.getName());

        /* TCP Headers's other computed information : */
        out.assertFieldExists(TCP_COMPUTED_SRCIP.getName());
        out.assertFieldEquals(TCP_COMPUTED_SRCIP.getName(), "192.168.10.226");

        out.assertFieldExists(TCP_COMPUTED_DSTIP.getName());
        out.assertFieldEquals(TCP_COMPUTED_DSTIP.getName(), "192.168.11.12");

        /* TODO */
        out.assertFieldExists(TCP_COMPUTED_SEGMENTTOTALLENGTH.getName());
        out.assertFieldEquals(TCP_COMPUTED_SEGMENTTOTALLENGTH.getName(), 20);

        /* TODO */
        out.assertFieldExists(TCP_COMPUTED_DATALENGTH.getName());
        out.assertFieldEquals(TCP_COMPUTED_DATALENGTH.getName(), 0);

        /* TODO */
        out.assertFieldExists(TCP_COMPUTED_REASSEMBLEDLENGTH.getName());
        out.assertFieldEquals(TCP_COMPUTED_REASSEMBLEDLENGTH.getName(), 0);

        /* TODO */
        //out.assertFieldExists(TCP_COMPUTED_TRAFFICDIRECTION.getName());
        //out.assertFieldEquals(TCP_COMPUTED_TRAFFICDIRECTION.getName(), " ??? ");

        /* TODO */
        out.assertFieldExists(TCP_COMPUTED_RELATIVEACK.getName());
        out.assertFieldEquals(TCP_COMPUTED_RELATIVEACK.getName(), 0);

        /* TODO */
        out.assertFieldExists(TCP_COMPUTED_RELATIVESEQ.getName());
        out.assertFieldEquals(TCP_COMPUTED_RELATIVESEQ.getName(), 0);
    }

    @Test
    public void testSmallPCapRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParsePCap());
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        try {
            byte[] pcapbytes = ProcessorsApiServiceImpl.loadFileContentAsBytes("pcapTestFiles/verySmallFlows.pcap");
            record.setField(FieldDictionary.RECORD_KEY, FieldType.LONG, 1338882754996790000L);
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(4);

        MockRecord out = testRunner.getOutputRecords().get(0);

        testSampleRecord(out);
    }

    @Test
    public void testTwoSmallPCapRecords() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParsePCap());
        testRunner.assertValid();
        Record record1 = new StandardRecord("pcap_event");
        Record record2 = new StandardRecord("pcap_event");
        System.out.println(System.getProperty("user.dir"));
        try {
            byte[] pcapbytes = ProcessorsApiServiceImpl.loadFileContentAsBytes("pcapTestFiles/verySmallFlows.pcap");
            record1.setField(FieldDictionary.RECORD_KEY, FieldType.LONG, 1338882754996790000L);
            record1.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);
            record2.setField(FieldDictionary.RECORD_KEY, FieldType.LONG, 1338882754996790000L);
            record2.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);
        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record1);
        testRunner.enqueue(record2);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(8);

        MockRecord out = testRunner.getOutputRecords().get(0);

        testSampleRecord(out);
    }

    @Test
    public void testMediumPCapRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParsePCap());
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        try {
            byte[] pcapbytes = ProcessorsApiServiceImpl.loadFileContentAsBytes("pcapTestFiles/mediumFlows.pcap");
            record.setField(FieldDictionary.RECORD_KEY, FieldType.LONG, 1338882754996790000L);
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(14261);

    }

    @Test
    public void testDummyPCapRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParsePCap());
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        byte[] pcapbytes = ProcessorsApiServiceImpl.loadFileContentAsBytes("pcapTestFiles/hurence_lake.pcap");
        try {
            record.setField(FieldDictionary.RECORD_KEY, FieldType.LONG, 1338882754996790000L);
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.RECORD_KEY);
        out.assertFieldEquals(FieldDictionary.RECORD_KEY, 1338882754996790000L);

        out.assertFieldExists(FieldDictionary.RECORD_VALUE);
        out.assertFieldEquals(FieldDictionary.RECORD_VALUE, pcapbytes);
    }
}
