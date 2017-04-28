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

package com.hurence.logisland.processor.networkpacket;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.file.FileUtil;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hurence.logisland.processor.networkpacket.Constants.*;
import static com.hurence.logisland.processor.networkpacket.PCapConstants.Fields.*;

/**
 * Test PCap processor.
 */
public class ParseNetworkPacketTest {
    
    private static Logger logger = LoggerFactory.getLogger(ParseNetworkPacketTest.class);

    private int testPCapFieldsValid(MockRecord out) {

        int numberOfValidatedFields = 0;

        ////////////////////////////
        // Standard Record Fields //
        ////////////////////////////

        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldTypeEquals(FieldDictionary.RECORD_TYPE, FieldType.STRING);
        numberOfValidatedFields++;

        out.assertFieldExists(FieldDictionary.PROCESSOR_NAME);
        out.assertFieldTypeEquals(FieldDictionary.PROCESSOR_NAME, FieldType.STRING);
        numberOfValidatedFields++;

        //////////////////////////
        // Global Header Fields //
        //////////////////////////

        out.assertFieldExists(GLOBAL_MAGICNUMBER.getName());
        out.assertFieldTypeEquals(GLOBAL_MAGICNUMBER.getName(), FieldType.INT);
        numberOfValidatedFields++;

        ////////////////////////
        // Packet Header data //
        ////////////////////////

        out.assertFieldExists(PCKT_TIMESTAMP_IN_NANOS.getName());
        out.assertFieldTypeEquals(PCKT_TIMESTAMP_IN_NANOS.getName(), FieldType.LONG);
        numberOfValidatedFields++;

        /* Only IPv4 protocol is handled so far : */
        if(out.getField(IP_PROTOCOL.getName()) != null && out.getField(IP_VERSION.getName()).asInteger().intValue() == PROTOCOL_IPV4) {

            //////////////////////
            // IP Header Fields //
            //////////////////////

            /* IP Header's 1st 32-bits word : */
            out.assertFieldExists(IP_VERSION.getName());
            out.assertFieldTypeEquals(IP_VERSION.getName(), FieldType.INT);
            numberOfValidatedFields++;

            out.assertFieldExists(IP_INTERNETHEADERLENGTH.getName());
            out.assertFieldTypeEquals(IP_INTERNETHEADERLENGTH.getName(), FieldType.INT);
            numberOfValidatedFields++;

            out.assertFieldExists(IP_TYPEOFSERVICE.getName());
            out.assertFieldTypeEquals(IP_TYPEOFSERVICE.getName(), FieldType.INT);
            numberOfValidatedFields++;

            out.assertFieldExists(IP_DATAGRAMTOTALLENGTH.getName());
            out.assertFieldTypeEquals(IP_DATAGRAMTOTALLENGTH.getName(), FieldType.INT);
            numberOfValidatedFields++;

            /* IP Header's 2nd 32-bits word : */
            out.assertFieldExists(IP_IDENTIFICATION.getName());
            out.assertFieldTypeEquals(IP_IDENTIFICATION.getName(), FieldType.INT);
            numberOfValidatedFields++;

            out.assertFieldExists(IP_FLAGS.getName());
            out.assertFieldTypeEquals(IP_FLAGS.getName(), FieldType.INT);
            numberOfValidatedFields++;

            out.assertFieldExists(IP_FRAGMENTOFFSET.getName());
            out.assertFieldTypeEquals(IP_FRAGMENTOFFSET.getName(), FieldType.INT);
            numberOfValidatedFields++;

            /* IP Header's 3rd 32-bits word : */
            out.assertFieldExists(IP_TIMETOLIVE.getName());
            out.assertFieldTypeEquals(IP_TIMETOLIVE.getName(), FieldType.INT);
            numberOfValidatedFields++;

            out.assertFieldExists(IP_PROTOCOL.getName());
            out.assertFieldTypeEquals(IP_PROTOCOL.getName(), FieldType.INT);
            numberOfValidatedFields++;

            out.assertFieldExists(IP_CHECKSUM.getName());
            out.assertFieldTypeEquals(IP_CHECKSUM.getName(), FieldType.INT);
            numberOfValidatedFields++;

            /* IP Header's 4th 32-bits word : */
            out.assertFieldExists(IP_SRCIPADDRESS.getName());
            out.assertFieldTypeEquals(IP_SRCIPADDRESS.getName(), FieldType.STRING);
            numberOfValidatedFields++;

            /* IP Header's 5th 32-bits word : */
            out.assertFieldExists(IP_DSTIPADDRESS.getName());
            out.assertFieldTypeEquals(IP_DSTIPADDRESS.getName(), FieldType.STRING);
            numberOfValidatedFields++;

            /* Only TCP and UDP protocols are handled so far : */
            if ((out.getField(IP_PROTOCOL.getName()).asInteger().intValue()) == PROTOCOL_TCP) {
                ///////////////////////
                // TCP Header Fields //
                ///////////////////////

                /* TCP Header's 1st 32-bits word : */
                out.assertFieldExists(TCP_SRCPORT.getName());
                out.assertFieldTypeEquals(TCP_SRCPORT.getName(), FieldType.INT);
                numberOfValidatedFields++;

                out.assertFieldExists(TCP_DSTPORT.getName());
                out.assertFieldTypeEquals(TCP_DSTPORT.getName(), FieldType.INT);
                numberOfValidatedFields++;

                /* TCP Header's 2nd 32-bits word : */
                out.assertFieldExists(TCP_SEQUENCENUMBER.getName());
                out.assertFieldTypeEquals(TCP_SEQUENCENUMBER.getName(), FieldType.INT);
                numberOfValidatedFields++;

                /* TCP Header's 3rd 32-bits word : */
                out.assertFieldExists(TCP_ACKNOWLEDGMENTNUMBER.getName());
                out.assertFieldTypeEquals(TCP_ACKNOWLEDGMENTNUMBER.getName(), FieldType.INT);
                numberOfValidatedFields++;

                /* TCP Header's 4th 32-bits word : */
                out.assertFieldExists(TCP_DATAOFFSET.getName());
                out.assertFieldTypeEquals(TCP_DATAOFFSET.getName(), FieldType.INT);
                numberOfValidatedFields++;

                out.assertFieldExists(TCP_FLAGS.getName());
                out.assertFieldTypeEquals(TCP_FLAGS.getName(), FieldType.INT);
                numberOfValidatedFields++;

                out.assertFieldExists(TCP_WINDOWSIZE.getName());
                out.assertFieldTypeEquals(TCP_WINDOWSIZE.getName(), FieldType.INT);
                numberOfValidatedFields++;

                /* TCP Header's 5th 32-bits word : */
                out.assertFieldExists(TCP_CHECKSUM.getName());
                out.assertFieldTypeEquals(TCP_CHECKSUM.getName(), FieldType.INT);
                numberOfValidatedFields++;

                out.assertFieldExists(TCP_URGENTPOINTER.getName());
                out.assertFieldTypeEquals(TCP_URGENTPOINTER.getName(), FieldType.INT);
                numberOfValidatedFields++;

                /* TCP Headers's following 32-bits word(s) : */


                /* TCP Headers's other computed information : */
                out.assertFieldExists(TCP_COMPUTED_SRCIP.getName());
                out.assertFieldTypeEquals(TCP_COMPUTED_SRCIP.getName(), FieldType.STRING);
                numberOfValidatedFields++;

                out.assertFieldExists(TCP_COMPUTED_DSTIP.getName());
                out.assertFieldTypeEquals(TCP_COMPUTED_DSTIP.getName(), FieldType.STRING);
                numberOfValidatedFields++;

                /* TODO */
                out.assertFieldExists(TCP_COMPUTED_SEGMENTTOTALLENGTH.getName());
                out.assertFieldTypeEquals(TCP_COMPUTED_SEGMENTTOTALLENGTH.getName(), FieldType.INT);
                numberOfValidatedFields++;

                /* TODO */
                out.assertFieldExists(TCP_COMPUTED_DATALENGTH.getName());
                out.assertFieldTypeEquals(TCP_COMPUTED_DATALENGTH.getName(), FieldType.INT);
                numberOfValidatedFields++;

                /* TODO */
                out.assertFieldExists(TCP_COMPUTED_REASSEMBLEDLENGTH.getName());
                out.assertFieldTypeEquals(TCP_COMPUTED_REASSEMBLEDLENGTH.getName(), FieldType.INT);
                numberOfValidatedFields++;

                /* TODO */
                //out.assertFieldExists(TCP_COMPUTED_TRAFFICDIRECTION.getName());
                //out.assertFieldTypeEquals(TCP_COMPUTED_TRAFFICDIRECTION.getName(),FieldType.STRING);
                //numberOfValidatedFields++;

                /* TODO */
                out.assertFieldExists(TCP_COMPUTED_RELATIVEACK.getName());
                out.assertFieldTypeEquals(TCP_COMPUTED_RELATIVEACK.getName(), FieldType.INT);
                numberOfValidatedFields++;

                /* TODO */
                out.assertFieldExists(TCP_COMPUTED_RELATIVESEQ.getName());
                out.assertFieldTypeEquals(TCP_COMPUTED_RELATIVESEQ.getName(), FieldType.INT);
                numberOfValidatedFields++;

            } else if ((out.getField(IP_PROTOCOL.getName()).asInteger().intValue()) == PROTOCOL_UDP) {
                ///////////////////////
                // UDP Header Fields //
                ///////////////////////

                /* UDP Header's 1st 32-bits word : */
                out.assertFieldExists(UDP_SRCPORT.getName());
                out.assertFieldTypeEquals(UDP_SRCPORT.getName(), FieldType.INT);
                numberOfValidatedFields++;

                out.assertFieldExists(UDP_DSTPORT.getName());
                out.assertFieldTypeEquals(UDP_DSTPORT.getName(), FieldType.INT);
                numberOfValidatedFields++;

                /* UDP Header's 2nd 32-bits word : */
                out.assertFieldExists(UDP_SEGMENTTOTALLENGTH.getName());
                out.assertFieldTypeEquals(UDP_SEGMENTTOTALLENGTH.getName(), FieldType.INT);
                numberOfValidatedFields++;

                out.assertFieldExists(UDP_CHECKSUM.getName());
                out.assertFieldTypeEquals(UDP_CHECKSUM.getName(), FieldType.INT);
                numberOfValidatedFields++;

            }
        }

        return numberOfValidatedFields ;
    }

    private void testSampleIPv4TCPPacketRecord(MockRecord out) {

        /////////////////////
        // Standard Fields //
        /////////////////////

        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "pcap_packet");

        out.assertFieldEquals(FieldDictionary.PROCESSOR_NAME, "ParseNetworkPacket");

        //////////////////////////
        // Global Header Fields //
        //////////////////////////

        //out.assertFieldEquals(PCKT_TIMESTAMP_IN_NANOS.getName(), 1338882754996790000L);

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
    public void test1TCPPacketsPCapRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParseNetworkPacket());
        testRunner.setProperty(ParseNetworkPacket.FLOW_MODE, "batch");
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        try {
            byte[] pcapbytes = FileUtil.loadFileContentAsBytes("pcapTestFiles/1-TCP-packet.pcap");

            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

            /*System.out.println("Length : " + pcapbytes.length);
            String pcapString = "";
            for(int i = 0; i<pcapbytes.length; i++)
            {
                pcapString = pcapString + ", " + pcapbytes[i];
            }
            System.out.println("pcapString : " + pcapString);*/

        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);

        MockRecord out = testRunner.getOutputRecords().get(0);

        int numberOfValidatedFields = testPCapFieldsValid(out);
        Assert.assertEquals(32,numberOfValidatedFields);
        testSampleIPv4TCPPacketRecord(out);
    }

    @Test
    public void test1TCPPacketsPCapRecord_Stream() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParseNetworkPacket());
        testRunner.setProperty(ParseNetworkPacket.FLOW_MODE, "stream");
        testRunner.assertValid();
        Record record1 = new StandardRecord("packet_event");
        Record record2 = new StandardRecord("packet_event");

        System.out.println(System.getProperty("user.dir"));
        try {
            final byte[] pcapbytes = new byte[] {0, 18, -49, -27, 84, -96, 0, 31, 60, 35, -37, -45, 8, 0,
                    69, 0, 0, 40, 74, -90, 64, 0, 64, 6, 88, -21, -64, -88, 10, -30, -64, -88, 11, 12, 76,
                    -5, 0, 23, -25, -54, -8, 88, 38, 19, 69, -34, 80, 17, 64, -57, 62, -90, 0, 0};

            record1.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);
            record2.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

            /*System.out.println("Length : " + pcapbytes.length);
            String pcapString = "";
            for(int i = 0; i<pcapbytes.length; i++)
            {
                pcapString = pcapString + ", " + pcapbytes[i];
            }
            System.out.println("pcapString : " + pcapString);*/

        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record1);
        testRunner.enqueue(record2);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.assertOutputErrorCount(0);

        MockRecord outRecord1 = testRunner.getOutputRecords().get(0);
        MockRecord outRecord2 = testRunner.getOutputRecords().get(1);

        int numberOfValidatedFields1 = testPCapFieldsValid(outRecord1);
        Assert.assertEquals(32,numberOfValidatedFields1);
        testSampleIPv4TCPPacketRecord(outRecord1);

        int numberOfValidatedFields2 = testPCapFieldsValid(outRecord2);
        Assert.assertEquals(32,numberOfValidatedFields2);
        testSampleIPv4TCPPacketRecord(outRecord2);
    }

    @Test
    public void test4TCPPacketsPCapRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParseNetworkPacket());
        testRunner.setProperty(ParseNetworkPacket.FLOW_MODE, "batch");
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        try {
            byte[] pcapbytes = FileUtil.loadFileContentAsBytes("pcapTestFiles/4-TCP-packets.pcap");
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);


        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(4);
        testRunner.assertOutputErrorCount(0);

        MockRecord out = testRunner.getOutputRecords().get(0);

        int numberOfValidatedFields = testPCapFieldsValid(out);
        Assert.assertEquals(32,numberOfValidatedFields);
        testSampleIPv4TCPPacketRecord(out);
    }

    @Test
    public void testARPPCapRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParseNetworkPacket());
        testRunner.setProperty(ParseNetworkPacket.FLOW_MODE, "batch");
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        try {
            byte[] pcapbytes = FileUtil.loadFileContentAsBytes("pcapTestFiles/1-ARP+1-TCP-packets.pcap");
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.assertOutputErrorCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        int numberOfValidatedFields = testPCapFieldsValid(out);
        Assert.assertEquals(4,numberOfValidatedFields);
    }

    @Test
    public void testTwoSmallSizePCapRecords() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParseNetworkPacket());
        testRunner.setProperty(ParseNetworkPacket.FLOW_MODE, "batch");
        testRunner.assertValid();
        Record record1 = new StandardRecord("pcap_event");
        Record record2 = new StandardRecord("pcap_event");
        System.out.println(System.getProperty("user.dir"));
        try {
            byte[] pcapbytes = FileUtil.loadFileContentAsBytes("pcapTestFiles/4-TCP-packets.pcap");
            record1.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);
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
        testSampleIPv4TCPPacketRecord(out);
    }

    @Test
    public void testMediumSizePCapRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParseNetworkPacket());
        testRunner.setProperty(ParseNetworkPacket.FLOW_MODE, "batch");
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        try {
            byte[] pcapbytes = FileUtil.loadFileContentAsBytes("pcapTestFiles/14261-packets.pcap");
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(14261);
        testRunner.assertOutputErrorCount(52);

        //int i = 0;
        for(MockRecord mockOutputRecord : testRunner.getOutputRecords() ) {
            //try {
                //i++;
                testPCapFieldsValid(mockOutputRecord);
            /*} catch (AssertionError e) {
                logger.error("AssertionError raised for packet number " + i + " : " + e.getMessage());
                Assert.fail();
            }*/
        }
    }

    @Test
    public void testDummyPCapRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParseNetworkPacket());
        testRunner.setProperty(ParseNetworkPacket.FLOW_MODE, "batch");
        testRunner.assertValid();
        Record record = new StandardRecord("pcap_event");

        System.out.println(System.getProperty("user.dir"));
        byte[] pcapbytes = FileUtil.loadFileContentAsBytes("pcapTestFiles/picture.pcap");
        try {
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, pcapbytes);

        } catch (Exception e) {
            e.printStackTrace();
        }

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();

        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.RECORD_VALUE);
        out.assertFieldEquals(FieldDictionary.RECORD_VALUE, pcapbytes);

        out.assertFieldExists(FieldDictionary.RECORD_ERRORS);
    }

}
