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
package com.hurence.logisland.processor.webanalytics;

import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URIDecoderTest  {

    public Processor getDecoder() {
        return new URIDecoder();
    }

    private static final String urlVal1 = "https://www.test.com/de/search/?text=toto";
    private static final String expectedDecodedUrlVal1 = urlVal1;

    private static final String urlVal2 = "https://www.test.com/de/search/?text=calendrier%20%20%202019";
    private static final String expectedDecodedUrlVal2 = "https://www.test.com/de/search/?text=calendrier   2019";

    private static final String urlVal3 = "https://www.test.com/en/search/?text=key1+%20key2%20+%28key3-key4%29";
    private static final String expectedDecodedUrlVal3 = "https://www.test.com/en/search/?text=key1+ key2 +(key3-key4)";

    private static final String val4 = "key1+%20key2%20+%28key3-key4%29";
    private static final String expectedDecodedVal4 = "key1+ key2 +(key3-key4)";

    private static final String val5 = "%co";

    private static final String val6 = "%%";

    private static final String value1 = "value1";
    private static final String value2 = "value2";


    private Record getRecord1() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, value1);
        record1.setField("string2", FieldType.STRING, value2);
        record1.setField("long1", FieldType.LONG, 1);
        record1.setField("long2", FieldType.LONG, 2);
        record1.setField("url1", FieldType.STRING, urlVal1);
        record1.setField("url2", FieldType.STRING, urlVal2);
        record1.setField("url3", FieldType.STRING, urlVal3);
        record1.setField("val4", FieldType.STRING, val4);
        return record1;
    }

    private Record getRecord2() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, value1);
        record1.setField("string2", FieldType.STRING, value2);
        record1.setField("long1", FieldType.LONG, 1);
        record1.setField("long2", FieldType.LONG, 2);
        record1.setField("url1", FieldType.STRING, urlVal1);
        record1.setField("url2", FieldType.STRING, urlVal2);
        record1.setField("val5", FieldType.STRING, val5);
        record1.setField("val6", FieldType.STRING, val6);
        return record1;
    }


    @Test
    public void testNoURLValueField() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(getDecoder());
        testRunner.setProperty("decode.fields", "string1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record1.size());
        out.assertFieldEquals("string1", value1);
        out.assertFieldTypeEquals("string1", FieldType.STRING);
    }

    @Test
    public void testBasicURLValueField() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(getDecoder());
        testRunner.setProperty("decode.fields", "string1, url1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record1.size());
        out.assertFieldEquals("string1", value1);
        out.assertFieldEquals("url1", expectedDecodedUrlVal1);
    }

    @Test
    public void testEncodedURLValueField() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(getDecoder());
        testRunner.setProperty("decode.fields", "url2,string1, url1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record1.size());
        out.assertFieldEquals("string1", value1);
        out.assertFieldEquals("url1", expectedDecodedUrlVal1);
        out.assertFieldEquals("url2", expectedDecodedUrlVal2);
    }

    @Test
    public void testComplexEncodedURLValueField() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(getDecoder());
        testRunner.setProperty("decode.fields", "url2,string1, url1,   url3");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record1.size());
        out.assertFieldEquals("string1", value1);
        out.assertFieldEquals("url1", expectedDecodedUrlVal1);
        out.assertFieldEquals("url2", expectedDecodedUrlVal2);
        out.assertFieldEquals("url3", expectedDecodedUrlVal3);
    }

    @Test
    public void testComplexEncodedValueField() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(getDecoder());
        testRunner.setProperty("decode.fields", "url2,string1, url1,  val4");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record1.size());
        out.assertFieldEquals("string1", value1);
        out.assertFieldEquals("url1", expectedDecodedUrlVal1);
        out.assertFieldEquals("url2", expectedDecodedUrlVal2);
        out.assertFieldEquals("val4", expectedDecodedVal4);
    }

    @Test
    public void testNoMatchingField() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(getDecoder());
        testRunner.setProperty("decode.fields", "nonExistingField");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record1.size());
        out.assertFieldEquals("string1", value1);
        out.assertFieldEquals("url1", urlVal1);
        out.assertFieldEquals("url2", urlVal2);
        out.assertFieldEquals("val4", val4);
    }

    @Test
    public void testPercentButNotHexaField() {

        Record record2 = getRecord2();

        TestRunner testRunner = TestRunners.newTestRunner(getDecoder());
        testRunner.setProperty("decode.fields", "val5,val6");
        testRunner.assertValid();
        testRunner.enqueue(record2);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
        testRunner.assertOutputErrorCount(1);

        MockRecord out = testRunner.getErrorRecords().get(0);
        Assert.assertEquals(2, out.getErrors().size());
    }
}
