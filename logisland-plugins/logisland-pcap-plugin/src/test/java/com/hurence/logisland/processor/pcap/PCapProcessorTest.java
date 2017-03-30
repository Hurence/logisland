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

import static com.hurence.logisland.processor.pcap.Constants.*;
import static com.hurence.logisland.processor.pcap.PCapConstants.Fields.*;

//import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test PCap processor.
 */
public class PCapProcessorTest {
    
    private static Logger logger = LoggerFactory.getLogger(PCapProcessorTest.class);

    private void testPCapFieldsValid(MockRecord out) {

        /////////////////////
        // Standard Fields //
        /////////////////////

        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldTypeEquals(FieldDictionary.RECORD_TYPE, FieldType.STRING);

        out.assertFieldExists(FieldDictionary.PROCESSOR_NAME);
        out.assertFieldTypeEquals(FieldDictionary.PROCESSOR_NAME, FieldType.STRING);

        //////////////////////////
        // Global Header Fields //
        //////////////////////////

        out.assertFieldExists(PCKT_TIMESTAMP_IN_NANOS.getName());
        out.assertFieldTypeEquals(PCKT_TIMESTAMP_IN_NANOS.getName(), FieldType.LONG);

        //////////////////////
        // IP Header Fields //
        //////////////////////

        /* IP Header's 1st 32-bits word : */
        out.assertFieldExists(IP_VERSION.getName());
        out.assertFieldTypeEquals(IP_VERSION.getName(), FieldType.INT);

        out.assertFieldExists(IP_INTERNETHEADERLENGTH.getName());
        out.assertFieldTypeEquals(IP_INTERNETHEADERLENGTH.getName(), FieldType.INT);

        out.assertFieldExists(IP_TYPEOFSERVICE.getName());
        out.assertFieldTypeEquals(IP_TYPEOFSERVICE.getName(), FieldType.INT);

        out.assertFieldExists(IP_DATAGRAMTOTALLENGTH.getName());
        out.assertFieldTypeEquals(IP_DATAGRAMTOTALLENGTH.getName(), FieldType.INT);

        /* IP Header's 2nd 32-bits word : */
        out.assertFieldExists(IP_IDENTIFICATION.getName());
        out.assertFieldTypeEquals(IP_IDENTIFICATION.getName(), FieldType.INT);

        out.assertFieldExists(IP_FLAGS.getName());
        out.assertFieldTypeEquals(IP_FLAGS.getName(), FieldType.INT);

        out.assertFieldExists(IP_FRAGMENTOFFSET.getName());
        out.assertFieldTypeEquals(IP_FRAGMENTOFFSET.getName(), FieldType.INT);

        /* IP Header's 3rd 32-bits word : */
        out.assertFieldExists(IP_TIMETOLIVE.getName());
        out.assertFieldTypeEquals(IP_TIMETOLIVE.getName(), FieldType.INT);

        out.assertFieldExists(IP_PROTOCOL.getName());
        out.assertFieldTypeEquals(IP_PROTOCOL.getName(), FieldType.INT);

        out.assertFieldExists(IP_CHECKSUM.getName());
        out.assertFieldTypeEquals(IP_CHECKSUM.getName(), FieldType.INT);

        /* IP Header's 4th 32-bits word : */
        out.assertFieldExists(IP_SRCIPADDRESS.getName());
        out.assertFieldTypeEquals(IP_SRCIPADDRESS.getName(), FieldType.STRING);

        /* IP Header's 5th 32-bits word : */
        out.assertFieldExists(IP_DSTIPADDRESS.getName());
        out.assertFieldTypeEquals(IP_DSTIPADDRESS.getName(), FieldType.STRING);


        if((out.getField(IP_PROTOCOL.getName()).asInteger().intValue()) == PROTOCOL_TCP)
        {
            ///////////////////////
            // TCP Header Fields //
            ///////////////////////

            /* TCP Header's 1st 32-bits word : */
            out.assertFieldExists(TCP_SRCPORT.getName());
            out.assertFieldTypeEquals(TCP_SRCPORT.getName(), FieldType.INT);

            out.assertFieldExists(TCP_DSTPORT.getName());
            out.assertFieldTypeEquals(TCP_DSTPORT.getName(), FieldType.INT);

            /* TCP Header's 2nd 32-bits word : */
            out.assertFieldExists(TCP_SEQUENCENUMBER.getName());
            out.assertFieldTypeEquals(TCP_SEQUENCENUMBER.getName(), FieldType.INT);

            /* TCP Header's 3rd 32-bits word : */
            out.assertFieldExists(TCP_ACKNOWLEDGMENTNUMBER.getName());
            out.assertFieldTypeEquals(TCP_ACKNOWLEDGMENTNUMBER.getName(), FieldType.INT);

            /* TCP Header's 4th 32-bits word : */
            out.assertFieldExists(TCP_DATAOFFSET.getName());
            out.assertFieldTypeEquals(TCP_DATAOFFSET.getName(), FieldType.INT);

            out.assertFieldExists(TCP_FLAGS.getName());
            out.assertFieldTypeEquals(TCP_FLAGS.getName(), FieldType.INT);

            out.assertFieldExists(TCP_WINDOWSIZE.getName());
            out.assertFieldTypeEquals(TCP_WINDOWSIZE.getName(), FieldType.INT);

            /* TCP Header's 5th 32-bits word : */
            out.assertFieldExists(TCP_CHECKSUM.getName());
            out.assertFieldTypeEquals(TCP_CHECKSUM.getName(), FieldType.INT);

            out.assertFieldExists(TCP_URGENTPOINTER.getName());
            out.assertFieldTypeEquals(TCP_URGENTPOINTER.getName(), FieldType.INT);

            /* TCP Headers's following 32-bits word(s) : */


            /* TCP Headers's other computed information : */
            out.assertFieldExists(TCP_COMPUTED_SRCIP.getName());
            out.assertFieldTypeEquals(TCP_COMPUTED_SRCIP.getName(), FieldType.STRING);

            out.assertFieldExists(TCP_COMPUTED_DSTIP.getName());
            out.assertFieldTypeEquals(TCP_COMPUTED_DSTIP.getName(), FieldType.STRING);

            /* TODO */
            out.assertFieldExists(TCP_COMPUTED_SEGMENTTOTALLENGTH.getName());
            out.assertFieldTypeEquals(TCP_COMPUTED_SEGMENTTOTALLENGTH.getName(), FieldType.INT);

            /* TODO */
            out.assertFieldExists(TCP_COMPUTED_DATALENGTH.getName());
            out.assertFieldTypeEquals(TCP_COMPUTED_DATALENGTH.getName(), FieldType.INT);

            /* TODO */
            out.assertFieldExists(TCP_COMPUTED_REASSEMBLEDLENGTH.getName());
            out.assertFieldTypeEquals(TCP_COMPUTED_REASSEMBLEDLENGTH.getName(), FieldType.INT);

            /* TODO */
            //out.assertFieldExists(TCP_COMPUTED_TRAFFICDIRECTION.getName());
            //out.assertFieldTypeEquals(TCP_COMPUTED_TRAFFICDIRECTION.getName(),FieldType.STRING);

            /* TODO */
            out.assertFieldExists(TCP_COMPUTED_RELATIVEACK.getName());
            out.assertFieldTypeEquals(TCP_COMPUTED_RELATIVEACK.getName(), FieldType.INT);

            /* TODO */
            out.assertFieldExists(TCP_COMPUTED_RELATIVESEQ.getName());
            out.assertFieldTypeEquals(TCP_COMPUTED_RELATIVESEQ.getName(), FieldType.INT);

        } else if((out.getField(IP_PROTOCOL.getName()).asInteger().intValue()) == PROTOCOL_UDP)
        {
            ///////////////////////
            // UDP Header Fields //
            ///////////////////////

            /* UDP Header's 1st 32-bits word : */
            out.assertFieldExists(UDP_SRCPORT.getName());
            out.assertFieldTypeEquals(UDP_SRCPORT.getName(), FieldType.INT);

            out.assertFieldExists(UDP_DSTPORT.getName());
            out.assertFieldTypeEquals(UDP_DSTPORT.getName(), FieldType.INT);

            /* UDP Header's 2nd 32-bits word : */
            out.assertFieldExists(UDP_SEGMENTTOTALLENGTH.getName());
            out.assertFieldTypeEquals(UDP_SEGMENTTOTALLENGTH.getName(), FieldType.INT);

            out.assertFieldExists(UDP_CHECKSUM.getName());
            out.assertFieldTypeEquals(UDP_CHECKSUM.getName(), FieldType.INT);
        }

    }

    private void testSampleTCPPacketRecord(MockRecord out) {

        /////////////////////
        // Standard Fields //
        /////////////////////

        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "network_packet");

        out.assertFieldEquals(FieldDictionary.PROCESSOR_NAME, "ParsePCap");

        //////////////////////////
        // Global Header Fields //
        //////////////////////////

        out.assertFieldEquals(PCKT_TIMESTAMP_IN_NANOS.getName(), 1338882754996790000L);

        //////////////////////
        // IP Header Fields //
        //////////////////////

        /* IP Header's 1st 32-bits word : */
        out.assertFieldEquals(IP_VERSION.getName(), 4);

        out.assertFieldEquals(IP_INTERNETHEADERLENGTH.getName(), 20);

        out.assertFieldEquals(IP_TYPEOFSERVICE.getName(), 0);

        out.assertFieldEquals(IP_DATAGRAMTOTALLENGTH.getName(), 40);

        /* IP Header's 2nd 32-bits word : */
        out.assertFieldEquals(IP_IDENTIFICATION.getName(), 19110);

        out.assertFieldEquals(IP_FLAGS.getName(), 2);

        out.assertFieldEquals(IP_FRAGMENTOFFSET.getName(), 0);

        /* IP Header's 3rd 32-bits word : */
        out.assertFieldEquals(IP_TIMETOLIVE.getName(), 64);

        out.assertFieldEquals(IP_PROTOCOL.getName(), 6);

        out.assertFieldEquals(IP_CHECKSUM.getName(), 22763);

        /* IP Header's 4th 32-bits word : */
        out.assertFieldEquals(IP_SRCIPADDRESS.getName(), "192.168.10.226");

        /* IP Header's 5th 32-bits word : */
        out.assertFieldEquals(IP_DSTIPADDRESS.getName(), "192.168.11.12");

        /* IP Headers's following 32-bits word(s) : */
        out.assertFieldNotExists(IP_OPTIONS.getName());

        out.assertFieldNotExists(IP_PADDING.getName());

        ///////////////////////
        // TCP Header Fields //
        ///////////////////////

        /* TCP Header's 1st 32-bits word : */
        out.assertFieldEquals(TCP_SRCPORT.getName(), 19707);

        out.assertFieldEquals(TCP_DSTPORT.getName(), 23);

        /* TCP Header's 2nd 32-bits word : */
        out.assertFieldEquals(TCP_SEQUENCENUMBER.getName(), -406128552);

        /* TCP Header's 3rd 32-bits word : */
        out.assertFieldEquals(TCP_ACKNOWLEDGMENTNUMBER.getName(), 638797278);

        /* TCP Header's 4th 32-bits word : */
        out.assertFieldEquals(TCP_DATAOFFSET.getName(), 5);

        out.assertFieldEquals(TCP_FLAGS.getName(), 17);

        out.assertFieldEquals(TCP_WINDOWSIZE.getName(), 16583);

        /* TCP Header's 5th 32-bits word : */
        out.assertFieldEquals(TCP_CHECKSUM.getName(), 16038);

        out.assertFieldEquals(TCP_URGENTPOINTER.getName(), 0);

        /* TCP Headers's following 32-bits word(s) : */
        out.assertFieldNotExists(TCP_OPTIONS.getName());

        out.assertFieldNotExists(TCP_PADDING.getName());

        /* TCP Headers's other computed information : */
        out.assertFieldEquals(TCP_COMPUTED_SRCIP.getName(), "192.168.10.226");

        out.assertFieldEquals(TCP_COMPUTED_DSTIP.getName(), "192.168.11.12");

        /* TODO */
        out.assertFieldEquals(TCP_COMPUTED_SEGMENTTOTALLENGTH.getName(), 20);

        /* TODO */
        out.assertFieldEquals(TCP_COMPUTED_DATALENGTH.getName(), 0);

        /* TODO */
        out.assertFieldEquals(TCP_COMPUTED_REASSEMBLEDLENGTH.getName(), 0);

        /* TODO */
        //out.assertFieldExists(TCP_COMPUTED_TRAFFICDIRECTION.getName());
        //out.assertFieldEquals(TCP_COMPUTED_TRAFFICDIRECTION.getName(), " ??? ");

        /* TODO */
        out.assertFieldEquals(TCP_COMPUTED_RELATIVEACK.getName(), 0);

        /* TODO */
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

        testPCapFieldsValid(out);
        testSampleTCPPacketRecord(out);
    }

    @Test
    public void testARPPCapRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParsePCap());
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        try {
            byte[] pcapbytes = ProcessorsApiServiceImpl.loadFileContentAsBytes("pcapTestFiles/1-ARP+1-TCP-IP.pcap");
            record.setField(FieldDictionary.RECORD_KEY, FieldType.LONG, 1338882754996790000L);
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);

        MockRecord out = testRunner.getOutputRecords().get(0);

        testPCapFieldsValid(out);
        //testSampleTCPPacketRecord(out);
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

        testPCapFieldsValid(out);
        testSampleTCPPacketRecord(out);
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

        int i = 0;
        //testRunner.getOutputRecords().forEach( mockOutputRecord -> {
        for(MockRecord mockOutputRecord : testRunner.getOutputRecords() ) {
            try {
                i++;
                testPCapFieldsValid(mockOutputRecord);
            } catch (AssertionError e) {
                logger.error("AssertionError raised for packet number " + i + " : " + e.getMessage());
                Assert.fail();
            }
        }
        //});
    }

    @Test
    public void testDummyPCapRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParsePCap());
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        byte[] pcapbytes = ProcessorsApiServiceImpl.loadFileContentAsBytes("pcapTestFiles/picture.pcap");
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
