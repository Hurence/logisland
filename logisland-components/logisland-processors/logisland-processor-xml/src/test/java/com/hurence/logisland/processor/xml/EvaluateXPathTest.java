/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.processor.xml;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test consolidate session processor.
 */
public class EvaluateXPathTest {

    private static Logger logger = LoggerFactory.getLogger(EvaluateXPathTest.class);

    @Test
    public void testOneBasicXPathRule() {

        Record record1 = new StandardRecord()
                .setField("attrSource", FieldType.STRING,
                        "<?xml version=\"1.0\"?><config><path>myvalue</path></config>")
                .setField("attr1", FieldType.STRING,"default");

        TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.SOURCE, "attrSource");
        testRunner.setProperty(EvaluateXPath.VALIDATE_DTD, "false");

        // Add only one dynamic property to the processor
        testRunner.setProperty("newAttr", "/config/path");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);
        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("newAttr","myvalue");
    }

    @Test
    public void testNonMatchingXPathRule() {

        Record record1 = new StandardRecord()
                .setField("attrSource", FieldType.STRING,
                        "<?xml version=\"1.0\"?><config><path>myvalue</path></config>")
                .setField("attr1", FieldType.STRING,"default");

        TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.SOURCE, "attrSource");
        testRunner.setProperty(EvaluateXPath.VALIDATE_DTD, "false");

        // Add only one dynamic property to the processor
        testRunner.setProperty("newAttr", "/config/unknown");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);
        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(2);
        out.assertFieldNotExists("newAttr");
    }

    @Test
    public void testTwoBasicXPathRules() {

        Record record1 = new StandardRecord()
                .setField("attrSource", FieldType.STRING,
                        "<?xml version=\"1.0\"?><config><path>myvalue</path><newpath>myvalue2</newpath></config>")
                .setField("attr1", FieldType.STRING,"default");

        TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.SOURCE, "attrSource");
        testRunner.setProperty(EvaluateXPath.VALIDATE_DTD, "false");

        // Add only one dynamic property to the processor
        testRunner.setProperty("newAttr", "/config/path");
        testRunner.setProperty("newAttr2", "/config/newpath");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);
        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("newAttr","myvalue");
        out.assertFieldEquals("newAttr2","myvalue2");
        out.assertRecordSizeEquals(4);
    }

    @Test
    public void testKeepOldValueAttrPlusXPathRules() {

        Record record1 = new StandardRecord()
                .setField("attrSource", FieldType.STRING,
                        "<?xml version=\"1.0\"?><config><path>myvalue</path><newpath>myvalue2</newpath></config>")
                .setField("newAttr", FieldType.STRING,"default");

        TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.SOURCE, "attrSource");
        testRunner.setProperty(EvaluateXPath.VALIDATE_DTD, "false");

        // Add only one dynamic property to the processor
        testRunner.setProperty("newAttr", "/config/path");
        testRunner.setProperty("newAttr2", "/config/newpath");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);
        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("newAttr","default");
        out.assertFieldEquals("newAttr2","myvalue2");
        out.assertRecordSizeEquals(3);
    }

    @Test
    public void testOverwriteValueAttrPlusXPathRules() {

        Record record1 = new StandardRecord()
                .setField("attrSource", FieldType.STRING,
                        "<?xml version=\"1.0\"?><config><path>myvalue</path><newpath>myvalue2</newpath></config>")
                .setField("newAttr", FieldType.STRING,"default");

        TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.SOURCE, "attrSource");
        testRunner.setProperty(EvaluateXPath.VALIDATE_DTD, "false");
        testRunner.setProperty(EvaluateXPath.CONFLICT_RESOLUTION_POLICY, "overwrite_existing");

        // Add only one dynamic property to the processor
        testRunner.setProperty("newAttr", "/config/path");
        testRunner.setProperty("newAttr2", "/config/newpath");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);
        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("newAttr","myvalue");
        out.assertFieldEquals("newAttr2","myvalue2");
        out.assertRecordSizeEquals(3);
    }
    @Test
    public void testOneComplexXPathRule() {

        Record record1 = new StandardRecord()
                .setField("attrSource", FieldType.STRING,
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                                "<FAP:Message xmlns:FAP=\"urn:test-com:TEST:FAP\">" +
                                "<FAP:Body>" +
                                "<lotId xmlns=\"urn:test-com:TEST:FAP:MES:txn\">Q836556</lotId>" +
                                "</FAP:Body>" +
                                "</FAP:Message>")
                .setField("attr1", FieldType.STRING,"default");


        TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.SOURCE, "attrSource");
        testRunner.setProperty(EvaluateXPath.VALIDATE_DTD, "false");

        // Add only one dynamic property to the processor
        // Select the first lotId (There is only one anyway)
        testRunner.setProperty("newAttr", "/*[local-name()='Message']/*[local-name()='Body']/*[name()='lotId']");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);
        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("newAttr","Q836556");
    }

    @Test
    public void testOneEvenComplexXPathRule() {

        Record record1 = new StandardRecord()
                .setField("attrSource", FieldType.STRING,
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                                "<FAP:Message xmlns:FAP=\"urn:test-com:TEST:FAP\">" +
                                "<FAP:Header xmlns=\"urn:test-com:TEST:FAP:MES:txn\">" +
                                  "<FAP:SenderID>300W</FAP:SenderID>" +
                                  "<FAP:ReceiverID>EXTERNALSYSTEM</FAP:ReceiverID>" +
                                  "<FAP:TID>749686529</FAP:TID>" +
                                  "<FAP:TimeStamp>20181005-10:15:53:194</FAP:TimeStamp>" +
                                  "<FAP:XSDVersion>0</FAP:XSDVersion>" +
                                  "<FAP:MsgTag>0</FAP:MsgTag>" +
                                  "<FAP:ApplDomain>300W</FAP:ApplDomain>" +
                                  "<FAP:TransactionName>FwStepChangeForLot</FAP:TransactionName>" +
                                  "<FAP:MsgType>BCAST</FAP:MsgType><FAP:ErrCode/>" +
                                  "<FAP:ErrMess/>" +
                                "</FAP:Header>" +
                                "<FAP:Body>" +
                                "<txn:FwStepChangeForLot xmlns:txn=\"urn:test-com:TEST:FAP:MES:txn\">" +
                                  "<lotId xmlns=\"urn:test-com:TEST:FAP:MES:txn\">Q836556</lotId>" +
                                  "<timeStamp xmlns=\"urn:test-com:TEST:FAP:MES:txn\">5349</timeStamp>" +
                                  "<stepId xmlns=\"urn:test-com:TEST:FAP:MES:txn\">IMPL_NWPIX2-01.0</stepId>" +
                                  "<stepHandle xmlns=\"urn:test-com:TEST:FAP:MES:txn\">240.1.30-IMPL_NWPIX2.1.10</stepHandle>" +
                                  "<currentRuleIndex xmlns=\"urn:test-com:TEST:FAP:MES:txn\">1</currentRuleIndex>" +
                                  "<stepProcessingStatus xmlns=\"urn:test-com:TEST:FAP:MES:txn\">Processing</stepProcessingStatus>" +
                                  "<stepDisplayStatus xmlns=\"urn:test-com:TEST:FAP:MES:txn\">ExecutingJOBPREP</stepDisplayStatus>" +
                                  "<location xmlns=\"urn:test-com:TEST:FAP:MES:txn\">IVISM01</location>" +
                                "</txn:FwStepChangeForLot>" +
                                "</FAP:Body>" +
                                "</FAP:Message>")
                .setField("attr1", FieldType.STRING,"default");

        TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.SOURCE, "attrSource");
        testRunner.setProperty(EvaluateXPath.VALIDATE_DTD, "false");

        // Add only one dynamic property to the processor
        //testRunner.setProperty("newAttr", "/*/*/*/*[name()='lotId']");
        testRunner.setProperty("newAttr", "//*[name()='lotId']");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);
        testRunner.assertOutputErrorCount(0);
        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("newAttr","Q836556");
    }
}
