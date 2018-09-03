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
package com.hurence.logisland.processor.netflow;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;

/**
 * Test simple Bro events processor.
 */
public class ParseNetflowEventTest {
    
    private static Logger logger = LoggerFactory.getLogger(ParseNetflowEventTest.class);
    
    // Bro conn input event
    private  final byte[] nfByterecord =  new byte[] {0, 5, 0, 2, 62, -128, 0, 69, 88, -20, -100, -22, 0, 1,
            -20, 48, 0, 0, 0, 0, 0, 0, 0, 0, -64, -88, 1, 26, 104, -12, 42, -63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, 52, 62, 126, -126, -33, 62, 126, -126, -33, -59, -114, 1, -69, 0, 16, 6, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 104, -12, 42, -63, -64, -88, 1, 26, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 52, 62,
            126, -126, -8, 62, 126, -126, -8, 1, -69, -59, -114, 0, 16, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    private String NETFLOW_EVENT ;

    @Test
    public void testNetFlowEvent() {

        //try {
            NETFLOW_EVENT = new String(nfByterecord);
        //} catch (UnsupportedEncodingException e) {
            //NETFLOW_EVENT = "";
        //}

        final TestRunner testRunner = TestRunners.newTestRunner(new ParseNetflowEvent());
        testRunner.assertValid();
        Record record = new StandardRecord("netflowevent");
        record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, nfByterecord);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "netflowevent");
        
        out.assertFieldExists("src_ip4");
        out.assertFieldEquals("src_ip4", "192.168.1.26");

        out.assertFieldExists("nexthop");
        out.assertFieldEquals("nexthop", "0.0.0.0");

        out.assertFieldExists("dst_as");
        out.assertFieldEquals("dst_as", (int) 0);

        out.assertFieldExists("dst_port");
        out.assertFieldEquals("dst_port", (int) 443);

        out.assertFieldExists("dst_ip4");
        out.assertFieldEquals("dst_ip4", "104.244.42.193");

        out.assertFieldExists("output");
        out.assertFieldEquals("output", (int) 0);

        out.assertFieldExists("nprot");
        out.assertFieldEquals("nprot", (int) 6);

        out.assertFieldExists("flags");
        out.assertFieldEquals("flags", (int) 16);

        out.assertFieldExists("dPkts");
        out.assertFieldEquals("dPkts", (int) 1);

        out.assertFieldExists("tos");
        out.assertFieldEquals("tos", (int) 0);

        out.assertFieldExists("src_as");
        out.assertFieldEquals("src_as", (int) 0);

        out.assertFieldExists("last");
        out.assertFieldEquals("last", (int) 1048478431);

        out.assertFieldExists("src_mask");
        out.assertFieldEquals("src_mask", (int) 0);

        out.assertFieldExists("dst_mask");
        out.assertFieldEquals("dst_mask", (int) 0);

        out.assertFieldExists("dOctets");
        out.assertFieldEquals("dOctets", (int) 52);

        out.assertFieldExists("input");
        out.assertFieldEquals("input", (int) 0);

        out.assertFieldExists("src_port");
        out.assertFieldEquals("src_port", (int) 50574);

        out.assertFieldExists("first");
        out.assertFieldEquals("first", (int) 1048478431);

        out = testRunner.getOutputRecords().get(1);
        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "netflowevent");

        out.assertFieldExists("src_ip4");
        out.assertFieldEquals("src_ip4", "104.244.42.193");

        out.assertFieldExists("dst_as");
        out.assertFieldEquals("dst_as", (int) 0);

        out.assertFieldExists("dst_port");
        out.assertFieldEquals("dst_port", (int) 50574);

        out.assertFieldExists("nexthop");
        out.assertFieldEquals("nexthop", "0.0.0.0");

        out.assertFieldExists("dst_as");
        out.assertFieldEquals("dst_as", (int) 0);

        out.assertFieldExists("dst_ip4");
        out.assertFieldEquals("dst_ip4", "192.168.1.26");

        out.assertFieldExists("output");
        out.assertFieldEquals("output", (int) 0);

        out.assertFieldExists("nprot");
        out.assertFieldEquals("nprot", (int) 6);

        out.assertFieldExists("flags");
        out.assertFieldEquals("flags", (int) 16);

        out.assertFieldExists("dPkts");
        out.assertFieldEquals("dPkts", (int) 1);

        out.assertFieldExists("tos");
        out.assertFieldEquals("tos", (int) 0);

        out.assertFieldExists("src_as");
        out.assertFieldEquals("src_as", (int) 0);

        out.assertFieldExists("last");
        out.assertFieldEquals("last", (int) 1048478456);

        out.assertFieldExists("src_mask");
        out.assertFieldEquals("src_mask", (int) 0);

        out.assertFieldExists("dst_mask");
        out.assertFieldEquals("dst_mask", (int) 0);

        out.assertFieldExists("dOctets");
        out.assertFieldEquals("dOctets", (int) 52);

        out.assertFieldExists("input");
        out.assertFieldEquals("input", (int) 0);

        out.assertFieldExists("src_port");
        out.assertFieldEquals("src_port", (int) 443);

        out.assertFieldExists("first");
        out.assertFieldEquals("first", (int) 1048478456);
    }
}
