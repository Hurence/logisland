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
package com.hurence.logisland.processor.webAnalytics;

import com.hurence.logisland.processor.webAnalytics.modele.SplittedURI;
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

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class URLCleanerTest {

    private static final Logger logger = LoggerFactory.getLogger(URLCleanerTest.class);

    private static final String url1 = "https://www.test.com/de/search/?text=toto";
    private static final String expectedUrl1WithoutParams = "https://www.test.com/de/search/";
    private static final String expectedUrl1KeepText = url1;
    private static final String expectedUrl1RemoveText = expectedUrl1WithoutParams;

    private static final String url2 = "https://www.t%888est%20.com/de/search/?text=calendrier%20%20%202019";
    private static final String expectedUrl2WithoutParams = "https://www.t%888est%20.com/de/search/";
//    private static final String expectedUrl2KeepText = "https://www.t%888est%20.com/de/search/?text=calendrier+++2019";
    private static final String expectedUrl2KeepText = url2;
    private static final String expectedUrl2RemoveText = expectedUrl2WithoutParams;

    private static final String url3 = "https://www.test.com/en/search/?text=key1+%20key2%20+%28key3-key4%29";
    private static final String expectedUrl3WithoutParams = "https://www.test.com/en/search/";
//    private static final String expectedUrl3KeepText = "https://www.test.com/en/search/?text=key1++key2++%28key3-key4%29";
    private static final String expectedUrl3KeepText = url3;
    private static final String expectedUrl3RemoveText = expectedUrl3WithoutParams;

    private static final String url4Decoded = "https://www.orexad.com/fr/search?q=sauterelle||relevance&page=2&sort=relevance";
    private static final String expectedUrl4DecodedWithoutParams = "https://www.orexad.com/fr/search";
    private static final String expectedUrl4DecodedKeepQ = "https://www.orexad.com/fr/search?q=sauterelle||relevance";
    private static final String expectedUrl4DecodedKeepPage = "https://www.orexad.com/fr/search?page=2";
    private static final String expectedUrl4DecodedRemoveQ = "https://www.orexad.com/fr/search?page=2&sort=relevance";
    private static final String expectedUrl4DecodedRemovePage = "https://www.orexad.com/fr/search?q=sauterelle||relevance&sort=relevance";

    private static final String url4 = "https://www.orexad.com/fr/search?q=sauterelle%7C%7Crelevance&page=2&sort=relevance";
    private static final String expectedUrl4WithoutParams = "https://www.orexad.com/fr/search";
    private static final String expectedUrl4KeepQ = "https://www.orexad.com/fr/search?q=sauterelle%7C%7Crelevance";
    private static final String expectedUrl4KeepPage = "https://www.orexad.com/fr/search?page=2";
    private static final String expectedUrl4RemoveQ = "https://www.orexad.com/fr/search?page=2&sort=relevance";
    private static final String expectedUrl4RemovePage = "https://www.orexad.com/fr/search?q=sauterelle%7C%7Crelevance&sort=relevance";

    private static final String url5 = "https://www.orexad.com/fr/sauterelle-inox-a-crochet-et-attache-a-levier-vertical/r-PR_G1050050542";
    private static final String expectedUrl5WithoutParams = "https://www.orexad.com/fr/sauterelle-inox-a-crochet-et-attache-a-levier-vertical/r-PR_G1050050542";

    private static final String val1 = "key1+%20key2%20+%28key3-key4%29";
    private static final String expectedVal1WithoutParams = "key1+%20key2%20+%28key3-key4%29";

    private static final String val2 = "%co";
    private static final String expectedVal5 = "%co";
    private static final String expectedVal2WithoutParams = "%co";

    private static final String val3 = "%%";
    private static final String expectedVal6 = "%%";
    private static final String expectedVal3WithoutParams = "%%";

    @Test
    public void testUriBuilder() throws URISyntaxException {
        SplittedURI url = SplittedURI.fromMalFormedURI("https://www.orexad.com/fr/search?q=sauterelle%7C%7Crelevance&page=2&sort=relevance");
//        private static final String url4 = "https://www.orexad.com/fr/search?q=sauterelle%7C%7Crelevance&page=2&sort=relevance";
        Assert.assertEquals("https://www.orexad.com/fr/search?", url.getBeforeQuery());
        Assert.assertEquals("q=sauterelle%7C%7Crelevance&page=2&sort=relevance", url.getQuery());
        Assert.assertEquals("", url.getAfterQuery());

        SplittedURI urlDecoded = SplittedURI.fromMalFormedURI("https://www.orexad.com/fr/search?q=sauterelle||relevance&page=2&sort=relevance");
//        private static final String url4Decoded = "https://www.orexad.com/fr/search?q=sauterelle||relevance&page=2&sort=relevance";
        Assert.assertEquals("https://www.orexad.com/fr/search?", urlDecoded.getBeforeQuery());
        Assert.assertEquals("q=sauterelle||relevance&page=2&sort=relevance", urlDecoded.getQuery());
        Assert.assertEquals("", urlDecoded.getAfterQuery());

        SplittedURI urlWithHashTag = SplittedURI.fromMalFormedURI("https://www.orexad.com/fr/search?q=sauterelle||relevance&page=2&sort=relevance#myTitle");
//        private static final String url4Decoded = "https://www.orexad.com/fr/search?q=sauterelle||relevance&page=2&sort=relevance";
        Assert.assertEquals("https://www.orexad.com/fr/search?", urlWithHashTag.getBeforeQuery());
        Assert.assertEquals("q=sauterelle||relevance&page=2&sort=relevance", urlWithHashTag.getQuery());
        Assert.assertEquals("#myTitle", urlWithHashTag.getAfterQuery());

        SplittedURI simpleUrl = SplittedURI.fromMalFormedURI("https://www.orexad.com/fr/");
//        private static final String url4Decoded = "https://www.orexad.com/fr/search?q=sauterelle||relevance&page=2&sort=relevance";
        Assert.assertEquals("https://www.orexad.com/fr/", simpleUrl.getBeforeQuery());
        Assert.assertEquals("", simpleUrl.getQuery());
        Assert.assertEquals("", simpleUrl.getAfterQuery());

        SplittedURI simpleUrlWithFragment = SplittedURI.fromMalFormedURI("https://www.orexad.com/fr/#gggg");
//        private static final String url4Decoded = "https://www.orexad.com/fr/search?q=sauterelle||relevance&page=2&sort=relevance";
        Assert.assertEquals("https://www.orexad.com/fr/", simpleUrlWithFragment.getBeforeQuery());
        Assert.assertEquals("", simpleUrlWithFragment.getQuery());
        Assert.assertEquals("#gggg", simpleUrlWithFragment.getAfterQuery());
    }


    private Record getRecord1() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("long1", FieldType.LONG, 1);
        record1.setField("long2", FieldType.LONG, 2);
        record1.setField("url1", FieldType.STRING, url1);
        record1.setField("url2", FieldType.STRING, url2);
        record1.setField("url3", FieldType.STRING, url3);
        record1.setField("url4", FieldType.STRING, url4);
        record1.setField("url4Decoded", FieldType.STRING, url4Decoded);
        record1.setField("url5", FieldType.STRING, url5);
        record1.setField("val1", FieldType.STRING, val1);
        record1.setField("val2", FieldType.STRING, val2);
        record1.setField("val3", FieldType.STRING, val3);
        return record1;
    }

    @Test
    public void testValidity() {
        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.assertNotValid();
        testRunner.setProperty(URLCleaner.URL_FIELDS, "string1");
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.REMOVE_ALL_PARAMS, "false");
        testRunner.assertNotValid();
        testRunner.setProperty(URLCleaner.REMOVE_ALL_PARAMS, "true");
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING.getValue());
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "a,b");
        testRunner.assertNotValid();
        testRunner.removeProperty(URLCleaner.REMOVE_PARAMS);
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.KEEP_PARAMS, "a,b");
        testRunner.assertNotValid();
        testRunner.removeProperty(URLCleaner.KEEP_PARAMS);
        testRunner.assertValid();
        testRunner.removeProperty(URLCleaner.REMOVE_ALL_PARAMS);
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.KEEP_PARAMS, "a,b");
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "a,b");
        testRunner.assertNotValid();
        testRunner.removeProperty(URLCleaner.KEEP_PARAMS);
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "");
        testRunner.assertNotValid();
        testRunner.removeProperty(URLCleaner.REMOVE_PARAMS);
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.URL_FIELDS, "");
        testRunner.assertNotValid();
        testRunner.setProperty(URLCleaner.URL_FIELDS, "a:g:v");
        testRunner.assertNotValid();
        testRunner.setProperty(URLCleaner.URL_FIELDS, "a:b,g:b,t");
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.URL_FIELDS, "b,g:b,t");
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.URL_FIELDS, "a,g,t");
        testRunner.assertValid();
        testRunner.setProperty(URLCleaner.URL_FIELDS, "a,b,g:b:h,t");
        testRunner.assertNotValid();
    }

    @Test
    public void testNoURLValueField() {
        Record record1 = getRecord1();
        int inputSize = record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "string1:newstring1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(inputSize + 1);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("newstring1", "value1");
        out.assertFieldTypeEquals("newstring1", FieldType.STRING);
    }

    @Test
    public void testNoURLValueField2() {
        Record record1 = getRecord1();
        int inputSize = record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "string1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(inputSize);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
    }

    @Test
    public void testBasicURLValueField() {
        Record record1 = getRecord1();
        int inputSize = record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "string1:newstring1,url1: new_url1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(inputSize + 2);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("newstring1", "value1");
        out.assertFieldTypeEquals("newstring1", FieldType.STRING);
        out.assertFieldEquals("url1", url1);
        out.assertFieldTypeEquals("url1", FieldType.STRING);
        out.assertFieldEquals("new_url1", expectedUrl1WithoutParams);
        out.assertFieldTypeEquals("new_url1", FieldType.STRING);
    }

    @Test
    public void testBasicURLValueField2() {
        Record record1 = getRecord1();
        int inputSize = record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "string1,url1");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(inputSize);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("url1", url1);
        out.assertFieldTypeEquals("url1", FieldType.STRING);
    }

    @Test
    public void testEncodedURLValueField() {
        Record record1 = getRecord1();
        int inputSize = record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url2,url1");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(inputSize);
        out.assertFieldEquals("url2", expectedUrl2WithoutParams);
        out.assertFieldTypeEquals("url2", FieldType.STRING);
        out.assertFieldEquals("url1", expectedUrl1WithoutParams);
        out.assertFieldTypeEquals("url1", FieldType.STRING);
    }


    @Test
    public void testComplexField() {
        Record record1 = getRecord1();
        int inputSize = record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url3,url4,url5,val1");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(inputSize);
        out.assertFieldEquals("url3", expectedUrl3WithoutParams);
        out.assertFieldEquals("url4", expectedUrl4WithoutParams);
        out.assertFieldEquals("url5", expectedUrl5WithoutParams);
        out.assertFieldEquals("val1", expectedVal1WithoutParams);
    }

    @Test
    public void testWithAlreadyDecodedUrls() {
        Record record1 = getRecord1();
        int inputSize = record1.size();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url4,url4Decoded,val2,val3");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord errorRecord = testRunner.getOutputRecords().get(0);
        errorRecord.assertRecordSizeEquals(inputSize);
        errorRecord.assertFieldEquals("url4", expectedUrl4WithoutParams);
        errorRecord.assertFieldEquals("val2", val2);
        errorRecord.assertFieldEquals("val3", val3);
        errorRecord.assertFieldEquals("url4Decoded", expectedUrl4DecodedWithoutParams);
    }

    @Test
    public void testNoMatchingField() {
        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "nonExistingField");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record1.size());
        out.assertContentEquals(record1);
    }


    @Test
    public void testPercentButNotHexaField() {
        Record record = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "val2,val3");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("val2", val2);
        out.assertFieldEquals("val3", val3);
    }

    @Test
    public void testKeepText() {
        Record record = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url1,url2,url3,url4,url4Decoded");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.KEEP_PARAMS, "text");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url1", expectedUrl1KeepText);
        out.assertFieldEquals("url2", expectedUrl2KeepText);
        out.assertFieldEquals("url3", expectedUrl3KeepText);
        out.assertFieldEquals("url4", expectedUrl4WithoutParams);
        out.assertFieldEquals("url4Decoded", expectedUrl4DecodedWithoutParams);
    }

    @Test
    public void testKeepQ() {
        Record record = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url1,url2,url3,url4,url4Decoded");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.KEEP_PARAMS, "q");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url1", expectedUrl1WithoutParams);
        out.assertFieldEquals("url2", expectedUrl2WithoutParams);
        out.assertFieldEquals("url3", expectedUrl3WithoutParams);
        out.assertFieldEquals("url4", expectedUrl4KeepQ);
        out.assertFieldEquals("url4Decoded", expectedUrl4DecodedKeepQ);
    }

    @Test
    public void testKeepPage() {
        Record record = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url1,url2,url3,url4,url4Decoded");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.KEEP_PARAMS, "page");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url1", expectedUrl1WithoutParams);
        out.assertFieldEquals("url2", expectedUrl2WithoutParams);
        out.assertFieldEquals("url3", expectedUrl3WithoutParams);
        out.assertFieldEquals("url4", expectedUrl4KeepPage);
        out.assertFieldEquals("url4Decoded", expectedUrl4DecodedKeepPage);
    }

    @Test
    public void testKeepTextAndQ() {
        Record record = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url1,url2,url3,url4,url4Decoded");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.KEEP_PARAMS, "text, q");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url1", expectedUrl1KeepText);
        out.assertFieldEquals("url2", expectedUrl2KeepText);
        out.assertFieldEquals("url3", expectedUrl3KeepText);
        out.assertFieldEquals("url4", expectedUrl4KeepQ);
        out.assertFieldEquals("url4Decoded", expectedUrl4DecodedKeepQ);
    }


    @Test
    public void testRemoveText() {
        Record record = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url1,url2,url3,url4,url4Decoded");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "text");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url1", expectedUrl1RemoveText);
        out.assertFieldEquals("url2", expectedUrl2RemoveText);
        out.assertFieldEquals("url3", expectedUrl3RemoveText);
        out.assertFieldEquals("url4", url4);
        out.assertFieldEquals("url4Decoded", url4Decoded);
    }

    @Test
    public void testRemoveQ() {
        Record record = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url1,url2,url3,url4,url4Decoded");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "q");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url1", expectedUrl1KeepText);
        out.assertFieldEquals("url2", expectedUrl2KeepText);
        out.assertFieldEquals("url3", expectedUrl3KeepText);
        out.assertFieldEquals("url4", expectedUrl4RemoveQ);
        out.assertFieldEquals("url4Decoded", expectedUrl4DecodedRemoveQ);
    }

    @Test
    public void testRemovePage() {
        Record record = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url1,url2,url3,url4,url4Decoded");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "page");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url1", expectedUrl1KeepText);
        out.assertFieldEquals("url2", expectedUrl2KeepText);
        out.assertFieldEquals("url3", expectedUrl3KeepText);
        out.assertFieldEquals("url4", expectedUrl4RemovePage);
        out.assertFieldEquals("url4Decoded", expectedUrl4DecodedRemovePage);
    }

    @Test
    public void testRemoveTextAndQ() {
        Record record = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url1,url2,url3,url4,url4Decoded");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "text, q");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url1", expectedUrl1RemoveText);
        out.assertFieldEquals("url2", expectedUrl2RemoveText);
        out.assertFieldEquals("url3", expectedUrl3RemoveText);
        out.assertFieldEquals("url4", expectedUrl4RemoveQ);
        out.assertFieldEquals("url4Decoded", expectedUrl4DecodedRemoveQ);
    }

    @Test
    public void specialTestDecodedQueryValuesInUrl() throws UnsupportedEncodingException {
        Record record = new StandardRecord();
        record.setField("url", FieldType.STRING, "https://mydomain.com/my/path/to/file.html?a=kaka&b=robl&oc&h=on#myfragment");
        record.setField("urlEncode", FieldType.STRING, "https://mydomain.com/my/path/to/file.html?a=kaka&b=" + URLEncoder.encode("robl&oc", StandardCharsets.UTF_8.toString()) + "&h=on#myfragment");

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url, urlEncode");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "b");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url", "https://mydomain.com/my/path/to/file.html?a=kaka&oc=&h=on#myfragment");
        out.assertFieldEquals("urlEncode", "https://mydomain.com/my/path/to/file.html?a=kaka&h=on#myfragment");
    }



}
