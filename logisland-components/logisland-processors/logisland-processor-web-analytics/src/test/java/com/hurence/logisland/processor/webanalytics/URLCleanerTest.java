/**
 * Copyright (C) 2020 Hurence (support@hurence.com)
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

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.webanalytics.modele.SplittedURI;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class URLCleanerTest {

    private static final String url1 = "https://www.test.com/de/search/?text=toto";
    private static final String expectedUrl1WithoutParams = "https://www.test.com/de/search/";
    private static final String expectedUrl1KeepText = url1;
    private static final String expectedUrl1RemoveText = expectedUrl1WithoutParams;

    private static final String url2 = "https://www.t%888est%20.com/de/search/?text=calendrier%20%20%202019";
    private static final String expectedUrl2WithoutParams = "https://www.t%888est%20.com/de/search/";
    private static final String expectedUrl2KeepText = url2;
    private static final String expectedUrl2RemoveText = expectedUrl2WithoutParams;

    private static final String url3 = "https://www.test.com/en/search/?text=key1+%20key2%20+%28key3-key4%29";
    private static final String expectedUrl3WithoutParams = "https://www.test.com/en/search/";
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
    private static final String val3 = "%%";


    @Test
    public void testSplittedUri() {
        SplittedURI url = SplittedURI.fromMalFormedURI("https://www.orexad.com/fr/search?q=sauterelle%7C%7Crelevance&page=2&sort=relevance");
        Assert.assertEquals("https://www.orexad.com/fr/search?", url.getBeforeQuery());
        Assert.assertEquals("q=sauterelle%7C%7Crelevance&page=2&sort=relevance", url.getQuery());
        Assert.assertEquals("", url.getAfterQuery());

        SplittedURI urlDecoded = SplittedURI.fromMalFormedURI("https://www.orexad.com/fr/search?q=sauterelle||relevance&page=2&sort=relevance");
        Assert.assertEquals("https://www.orexad.com/fr/search?", urlDecoded.getBeforeQuery());
        Assert.assertEquals("q=sauterelle||relevance&page=2&sort=relevance", urlDecoded.getQuery());
        Assert.assertEquals("", urlDecoded.getAfterQuery());

        SplittedURI urlWithHashTag = SplittedURI.fromMalFormedURI("https://www.orexad.com/fr/search?q=sauterelle||relevance&page=2&sort=relevance#myTitle");
        Assert.assertEquals("https://www.orexad.com/fr/search?", urlWithHashTag.getBeforeQuery());
        Assert.assertEquals("q=sauterelle||relevance&page=2&sort=relevance", urlWithHashTag.getQuery());
        Assert.assertEquals("#myTitle", urlWithHashTag.getAfterQuery());

        SplittedURI simpleUrl = SplittedURI.fromMalFormedURI("https://www.orexad.com/fr/");
        Assert.assertEquals("https://www.orexad.com/fr/", simpleUrl.getBeforeQuery());
        Assert.assertEquals("", simpleUrl.getQuery());
        Assert.assertEquals("", simpleUrl.getAfterQuery());

        SplittedURI simpleUrlWithFragment = SplittedURI.fromMalFormedURI("https://www.orexad.com/fr/#gggg");
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
        out.assertFieldEquals("url", "https://mydomain.com/my/path/to/file.html?a=kaka&oc&h=on#myfragment");
        out.assertFieldEquals("urlEncode", "https://mydomain.com/my/path/to/file.html?a=kaka&h=on#myfragment");
    }


    @Test
    public void testNullField() {
        Record record = new StandardRecord();
        record.setField("url", FieldType.STRING, null);

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "b");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertNullField("url");
    }

    @Test
    public void testEmptyField() {
        Record record = new StandardRecord();
        record.setField("url", FieldType.STRING, "");

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "b");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url","");
    }

    @Test
    public void testJustKeyInQuery() {
        Record record = new StandardRecord();
        record.setField("url", FieldType.STRING, "http://host.com/path?mysyntax&pretty&size=2#anchor");

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "mysyntax");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url","http://host.com/path?pretty&size=2#anchor");
    }

    @Test
    public void testJustKeyInQuery2() {
        Record record = new StandardRecord();
        record.setField("url", FieldType.STRING, "http://host.com/path?mysyntax&pretty&size=2#anchor");

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "pretty");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url","http://host.com/path?mysyntax&size=2#anchor");
    }

    @Test
    public void testQuerywithoutValueAndEmptyValue() {
        Record record = new StandardRecord();
        record.setField("url", FieldType.STRING, "http://host.com/path?a=b&c=&d&z=w");

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, "url");
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        testRunner.setProperty(URLCleaner.REMOVE_PARAMS, "pretty");
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());
        out.assertFieldEquals("url","http://host.com/path?a=b&c=&d&z=w");
    }

    @Test
    public void bulkRemoveAllTest() {
        Map<String, String> inputUrlToExpectedUrl = new HashMap<String, String>();
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/search/?text=loupe+las33300",
                "https://mydomain.com/fr/search/"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/elingue-ronde-haute-resistance/r-PR_G1408003386?q=elingue%7C%7Crelevance%7C%7CmanufacturerNameFacet%7C%7CGISS&text=elingue&classif=",
                "https://mydomain.com/fr/elingue-ronde-haute-resistance/r-PR_G1408003386"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/protection-usage-court/c-40-10-21?q=%7C%7Crelevance&page=6",
                "https://mydomain.com/fr/protection-usage-court/c-40-10-21"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/ensemble-ebavurage-chanfreinage-en-coffret/p-G1111003763",
                "https://mydomain.com/fr/ensemble-ebavurage-chanfreinage-en-coffret/p-G1111003763"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/bac-a-bec/r-PR_G1408000200?q=||||attribute157||Gris||attribute228||225x150x125%20mm||&classif=45&sortAttribute=&sortOrder=",
                "https://mydomain.com/fr/bac-a-bec/r-PR_G1408000200"
        );
//        decoded -> https://mydomain.com/fr/bac-a-bec/r-PR_G1408000200?q=||||attribute157||Gris||attribute228||225x150x125 mm||&classif=45&sortAttribute=&sortOrder=
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/search/?text=Chaine+9.25+inox",
                "https://mydomain.com/fr/search/"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/moraillon-porte-cadenas/p-G1164000013?t=cadenas+pompier",
                "https://mydomain.com/fr/moraillon-porte-cadenas/p-G1164000013"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/facom/c-1111?q=%7C%7C%7C%7CcategoryLevel1%7C%7C45&page=5",
                "https://mydomain.com/fr/facom/c-1111"
        );
        inputUrlToExpectedUrl.put(
                "https://www.btshop.nl/nl/veiligheidsschoen-sl-80-blue-esd-s2/r-PR_G1021004878?q=||relevance||attribute891||43&sortAttribute=attribute891,attribute109,attribute5041,attribute157&sortOrder=asc",
                "https://www.btshop.nl/nl/veiligheidsschoen-sl-80-blue-esd-s2/r-PR_G1021004878"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/cart#",
                "https://mydomain.com/fr/cart#"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/search/?text=smc+aq+240f-06-00",
                "https://mydomain.com/fr/search/"
        );
        inputUrlToExpectedUrl.put(
                "https://es.world.com/es/herramientas-de-mano/c-35-10?q=||relevance||manufacturerNameFacet||Roebuck||categoryLevel3%7C%7C35-10-20",
                "https://es.world.com/es/herramientas-de-mano/c-35-10"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/search/?text=piquage%20&q=piquage%20||relevance||manufacturerNameFacet||Parker%20Legris||attribute1875%7C%7CM5%20%27%27",
                "https://mydomain.com/search/"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractContent.aspx?abstractID=9GrIawodOyWmVAXM%2b9Bq3eJFWUiAKhB2Toh3Oct0zIH%2fCbISTIls4l4Ox45ROTAWHCUzXjOonos%3d",
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractContent.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congress/ESC-CONGRESS-2019/Expert-Advice-Tips-and-tricks-in-imaging-your-patient-with-valvular-heart-d/189866-tips-and-tricks-for-imaging-in-aortic-stenosis#video",
                "https://mydomain.com/Congress/ESC-CONGRESS-2019/Expert-Advice-Tips-and-tricks-in-imaging-your-patient-with-valvular-heart-d/189866-tips-and-tricks-for-imaging-in-aortic-stenosis#video"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Councils/Council-on-Hypertension-(CHT)/News/position-statement-of-the-world-council-on-hypertension-on-ace-inhibitors-and-ang",
                "https://mydomain.com/Councils/Council-on-Hypertension-(CHT)/News/position-statement-of-the-world-council-on-hypertension-on-ace-inhibitors-and-ang"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Journals/E-Journal-of-Cardiology-Practice/Volume-14/Treatment-of-right-heart-failure-is-there-a-solution-to-the-problem",
                "https://mydomain.com/Journals/E-Journal-of-Cardiology-Practice/Volume-14/Treatment-of-right-heart-failure-is-there-a-solution-to-the-problem"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congress/EuroCMR-2019/Special-Course-8-how-to-read-CMR-for-extra-cardiac-findings/187189-how-to-interpret-common-organ-specific-findings-in-the-lungs-skeletal-system-liver-kidneys-breast-case-based-interactive-discussion#slide",
                "https://mydomain.com/Congress/EuroCMR-2019/Special-Course-8-how-to-read-CMR-for-extra-cardiac-findings/187189-how-to-interpret-common-organ-specific-findings-in-the-lungs-skeletal-system-liver-kidneys-breast-case-based-interactive-discussion#slide"
        );
        inputUrlToExpectedUrl.put(
                "https://my--domain.force.org/apex/ESCUserProfileInfo",
                "https://my--domain.force.org/apex/ESCUserProfileInfo"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Education/COVID-19-and-Cardiology/ESC-COVID-19-Guidance?hit=home&urlorig=/vgn-ext-templating/#tbl07",
                "https://mydomain.com/Education/COVID-19-and-Cardiology/ESC-COVID-19-Guidance#tbl07"
        );
        inputUrlToExpectedUrl.put(
                "https://my--domain.force.org/ESCMyProfile?utm_medium=Email&utm_source=&utm_campaign=ESC+-+ESC+Congress+2020+-+registration+confirmation#",
                "https://my--domain.force.org/ESCMyProfile#"
        );
        inputUrlToExpectedUrl.put(
                "http://spo.hurence.org/default.aspx?eevtid=1482&showResults=False&_ga=2.267591435.481963044.1578999296-479375258.1578999295",
                "http://spo.hurence.org/default.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congresses-&-Events/Frontiers-in-Cardiovascular-Biomedicine/Registration?utm_medium=Email&utm_source=Councils&utm_campaign=Councils+-+FCVB+2020+-+Early+registration+fee+-++Last+call",
                "https://mydomain.com/Congresses-&-Events/Frontiers-in-Cardiovascular-Biomedicine/Registration"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractAuthors.aspx?abstractID=9GrIawodOyXZLPpXgJHtvCxG5gTt5TznJt97rA1Jy%2bzH7V5eLZVqUnyoo903fiw9nf7mbxKuI14%3d",
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractAuthors.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congresses-&-Events/ESC-Congress/Scientific-sessions",
                "https://mydomain.com/Congresses-&-Events/ESC-Congress/Scientific-sessions"
        );
        inputUrlToExpectedUrl.put(
                "https://idp.hurence.org/idp/login.jsp?loginFailed=true&actionUrl=%2Fidp%2FAuthn%2FESCUserPassword",
                "https://idp.hurence.org/idp/login.jsp"
        );
        inputUrlToExpectedUrl.put(
                "https://aa.net/2016/formulaResult.aspx?model=europelow&exam=&patient=370532",
                "https://aa.net/2016/formulaResult.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://aa.net/login.aspx?ReturnUrl=%2fpercutaneous-interventions%2fhomepage.aspx",
                "https://aa.net/login.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Education/Practice-Tools/EACVI-toolboxes/3D-Echo/Atlas-of-Three%E2%80%93dimensional-Echocardiography/Volumes-and-Ejection-Fraction",
                "https://mydomain.com/Education/Practice-Tools/EACVI-toolboxes/3D-Echo/Atlas-of-Three%E2%80%93dimensional-Echocardiography/Volumes-and-Ejection-Fraction"
        );
        inputUrlToExpectedUrl.put(
                "https://my--domain.force.org/ESCMyPublications",
                "https://my--domain.force.org/ESCMyPublications"
        );
        runTestWithRemoveAll(inputUrlToExpectedUrl);
    }

    @Test
    public void bulkRemoveQTest() {
        Map<String, String> inputUrlToExpectedUrl = new HashMap<String, String>();
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/search/?text=loupe+las33300",
                "https://mydomain.com/fr/search/?text=loupe+las33300"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/elingue-ronde-haute-resistance/r-PR_G1408003386?q=elingue%7C%7Crelevance%7C%7CmanufacturerNameFacet%7C%7CGISS&text=elingue&classif=",
                "https://mydomain.com/fr/elingue-ronde-haute-resistance/r-PR_G1408003386?text=elingue&classif="
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/protection-usage-court/c-40-10-21?q=%7C%7Crelevance&page=6",
                "https://mydomain.com/fr/protection-usage-court/c-40-10-21?page=6"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/ensemble-ebavurage-chanfreinage-en-coffret/p-G1111003763",
                "https://mydomain.com/fr/ensemble-ebavurage-chanfreinage-en-coffret/p-G1111003763"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/bac-a-bec/r-PR_G1408000200?q=||||attribute157||Gris||attribute228||225x150x125%20mm||&classif=45&sortAttribute=&sortOrder=",
                "https://mydomain.com/fr/bac-a-bec/r-PR_G1408000200?classif=45&sortAttribute=&sortOrder="
        );
//        decoded -> https://mydomain.com/fr/bac-a-bec/r-PR_G1408000200?q=||||attribute157||Gris||attribute228||225x150x125 mm||&classif=45&sortAttribute=&sortOrder=
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/search/?text=Chaine+9.25+inox",
                "https://mydomain.com/fr/search/?text=Chaine+9.25+inox"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/moraillon-porte-cadenas/p-G1164000013?t=cadenas+pompier",
                "https://mydomain.com/fr/moraillon-porte-cadenas/p-G1164000013?t=cadenas+pompier"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/facom/c-1111?q=%7C%7C%7C%7CcategoryLevel1%7C%7C45&page=5",
                "https://mydomain.com/fr/facom/c-1111?page=5"
        );
        inputUrlToExpectedUrl.put(
                "https://www.btshop.nl/nl/veiligheidsschoen-sl-80-blue-esd-s2/r-PR_G1021004878?q=||relevance||attribute891||43&sortAttribute=attribute891,attribute109,attribute5041,attribute157&sortOrder=asc",
                "https://www.btshop.nl/nl/veiligheidsschoen-sl-80-blue-esd-s2/r-PR_G1021004878?sortAttribute=attribute891,attribute109,attribute5041,attribute157&sortOrder=asc"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/cart#",
                "https://mydomain.com/fr/cart#"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/search/?text=smc+aq+240f-06-00",
                "https://mydomain.com/fr/search/?text=smc+aq+240f-06-00"
        );
        inputUrlToExpectedUrl.put(
                "https://es.world.com/es/herramientas-de-mano/c-35-10?q=||relevance||manufacturerNameFacet||Roebuck||categoryLevel3%7C%7C35-10-20",
                "https://es.world.com/es/herramientas-de-mano/c-35-10"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/search/?text=piquage%20&q=piquage%20||relevance||manufacturerNameFacet||Parker%20Legris||attribute1875%7C%7CM5%20%27%27",
                "https://mydomain.com/search/?text=piquage%20"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractContent.aspx?abstractID=9GrIawodOyWmVAXM%2b9Bq3eJFWUiAKhB2Toh3Oct0zIH%2fCbISTIls4l4Ox45ROTAWHCUzXjOonos%3d",
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractContent.aspx?abstractID=9GrIawodOyWmVAXM%2b9Bq3eJFWUiAKhB2Toh3Oct0zIH%2fCbISTIls4l4Ox45ROTAWHCUzXjOonos%3d"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congress/ESC-CONGRESS-2019/Expert-Advice-Tips-and-tricks-in-imaging-your-patient-with-valvular-heart-d/189866-tips-and-tricks-for-imaging-in-aortic-stenosis#video",
                "https://mydomain.com/Congress/ESC-CONGRESS-2019/Expert-Advice-Tips-and-tricks-in-imaging-your-patient-with-valvular-heart-d/189866-tips-and-tricks-for-imaging-in-aortic-stenosis#video"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Councils/Council-on-Hypertension-(CHT)/News/position-statement-of-the-world-council-on-hypertension-on-ace-inhibitors-and-ang",
                "https://mydomain.com/Councils/Council-on-Hypertension-(CHT)/News/position-statement-of-the-world-council-on-hypertension-on-ace-inhibitors-and-ang"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Journals/E-Journal-of-Cardiology-Practice/Volume-14/Treatment-of-right-heart-failure-is-there-a-solution-to-the-problem",
                "https://mydomain.com/Journals/E-Journal-of-Cardiology-Practice/Volume-14/Treatment-of-right-heart-failure-is-there-a-solution-to-the-problem"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congress/EuroCMR-2019/Special-Course-8-how-to-read-CMR-for-extra-cardiac-findings/187189-how-to-interpret-common-organ-specific-findings-in-the-lungs-skeletal-system-liver-kidneys-breast-case-based-interactive-discussion#slide",
                "https://mydomain.com/Congress/EuroCMR-2019/Special-Course-8-how-to-read-CMR-for-extra-cardiac-findings/187189-how-to-interpret-common-organ-specific-findings-in-the-lungs-skeletal-system-liver-kidneys-breast-case-based-interactive-discussion#slide"
        );
        inputUrlToExpectedUrl.put(
                "https://my--domain.force.org/apex/ESCUserProfileInfo",
                "https://my--domain.force.org/apex/ESCUserProfileInfo"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Education/COVID-19-and-Cardiology/ESC-COVID-19-Guidance?hit=home&urlorig=/vgn-ext-templating/#tbl07",
                "https://mydomain.com/Education/COVID-19-and-Cardiology/ESC-COVID-19-Guidance?hit=home&urlorig=/vgn-ext-templating/#tbl07"
        );
        inputUrlToExpectedUrl.put(
                "https://my--domain.force.org/ESCMyProfile?utm_medium=Email&utm_source=&utm_campaign=ESC+-+ESC+Congress+2020+-+registration+confirmation#",
                "https://my--domain.force.org/ESCMyProfile?utm_medium=Email&utm_source=&utm_campaign=ESC+-+ESC+Congress+2020+-+registration+confirmation#"
        );
        inputUrlToExpectedUrl.put(
                "http://spo.hurence.org/default.aspx?eevtid=1482&showResults=False&_ga=2.267591435.481963044.1578999296-479375258.1578999295",
                "http://spo.hurence.org/default.aspx?eevtid=1482&showResults=False&_ga=2.267591435.481963044.1578999296-479375258.1578999295"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congresses-&-Events/Frontiers-in-Cardiovascular-Biomedicine/Registration?utm_medium=Email&utm_source=Councils&utm_campaign=Councils+-+FCVB+2020+-+Early+registration+fee+-++Last+call",
                "https://mydomain.com/Congresses-&-Events/Frontiers-in-Cardiovascular-Biomedicine/Registration?utm_medium=Email&utm_source=Councils&utm_campaign=Councils+-+FCVB+2020+-+Early+registration+fee+-++Last+call"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractAuthors.aspx?abstractID=9GrIawodOyXZLPpXgJHtvCxG5gTt5TznJt97rA1Jy%2bzH7V5eLZVqUnyoo903fiw9nf7mbxKuI14%3d",
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractAuthors.aspx?abstractID=9GrIawodOyXZLPpXgJHtvCxG5gTt5TznJt97rA1Jy%2bzH7V5eLZVqUnyoo903fiw9nf7mbxKuI14%3d"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congresses-&-Events/ESC-Congress/Scientific-sessions",
                "https://mydomain.com/Congresses-&-Events/ESC-Congress/Scientific-sessions"
        );
        inputUrlToExpectedUrl.put(
                "https://idp.hurence.org/idp/login.jsp?loginFailed=true&actionUrl=%2Fidp%2FAuthn%2FESCUserPassword",
                "https://idp.hurence.org/idp/login.jsp?loginFailed=true&actionUrl=%2Fidp%2FAuthn%2FESCUserPassword"
        );
        inputUrlToExpectedUrl.put(
                "https://aa.net/2016/formulaResult.aspx?model=europelow&exam=&patient=370532",
                "https://aa.net/2016/formulaResult.aspx?model=europelow&exam=&patient=370532"
        );
        inputUrlToExpectedUrl.put(
                "https://aa.net/login.aspx?ReturnUrl=%2fpercutaneous-interventions%2fhomepage.aspx",
                "https://aa.net/login.aspx?ReturnUrl=%2fpercutaneous-interventions%2fhomepage.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Education/Practice-Tools/EACVI-toolboxes/3D-Echo/Atlas-of-Three%E2%80%93dimensional-Echocardiography/Volumes-and-Ejection-Fraction",
                "https://mydomain.com/Education/Practice-Tools/EACVI-toolboxes/3D-Echo/Atlas-of-Three%E2%80%93dimensional-Echocardiography/Volumes-and-Ejection-Fraction"
        );
        inputUrlToExpectedUrl.put(
                "https://my--domain.force.org/ESCMyPublications",
                "https://my--domain.force.org/ESCMyPublications"
        );
        runTestWithRemoveQ(inputUrlToExpectedUrl);
    }


    @Test
    public void bulkKeepQTest() {
        Map<String, String> inputUrlToExpectedUrl = new HashMap<String, String>();
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/search/?text=loupe+las33300",
                "https://mydomain.com/fr/search/"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/elingue-ronde-haute-resistance/r-PR_G1408003386?q=elingue%7C%7Crelevance%7C%7CmanufacturerNameFacet%7C%7CGISS&text=elingue&classif=",
                "https://mydomain.com/fr/elingue-ronde-haute-resistance/r-PR_G1408003386?q=elingue%7C%7Crelevance%7C%7CmanufacturerNameFacet%7C%7CGISS"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/protection-usage-court/c-40-10-21?q=%7C%7Crelevance&page=6",
                "https://mydomain.com/fr/protection-usage-court/c-40-10-21?q=%7C%7Crelevance"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/ensemble-ebavurage-chanfreinage-en-coffret/p-G1111003763",
                "https://mydomain.com/fr/ensemble-ebavurage-chanfreinage-en-coffret/p-G1111003763"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/bac-a-bec/r-PR_G1408000200?q=||||attribute157||Gris||attribute228||225x150x125%20mm||&classif=45&sortAttribute=&sortOrder=",
                "https://mydomain.com/fr/bac-a-bec/r-PR_G1408000200?q=||||attribute157||Gris||attribute228||225x150x125%20mm||"
        );
//        decoded -> https://mydomain.com/fr/bac-a-bec/r-PR_G1408000200?q=||||attribute157||Gris||attribute228||225x150x125 mm||&classif=45&sortAttribute=&sortOrder=
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/search/?text=Chaine+9.25+inox",
                "https://mydomain.com/fr/search/"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/moraillon-porte-cadenas/p-G1164000013?t=cadenas+pompier",
                "https://mydomain.com/fr/moraillon-porte-cadenas/p-G1164000013"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/facom/c-1111?q=%7C%7C%7C%7CcategoryLevel1%7C%7C45&page=5",
                "https://mydomain.com/fr/facom/c-1111?q=%7C%7C%7C%7CcategoryLevel1%7C%7C45"
        );
        inputUrlToExpectedUrl.put(
                "https://www.btshop.nl/nl/veiligheidsschoen-sl-80-blue-esd-s2/r-PR_G1021004878?q=||relevance||attribute891||43&sortAttribute=attribute891,attribute109,attribute5041,attribute157&sortOrder=asc",
                "https://www.btshop.nl/nl/veiligheidsschoen-sl-80-blue-esd-s2/r-PR_G1021004878?q=||relevance||attribute891||43"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/cart#",
                "https://mydomain.com/fr/cart#"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/fr/search/?text=smc+aq+240f-06-00",
                "https://mydomain.com/fr/search/"
        );
        inputUrlToExpectedUrl.put(
                "https://es.world.com/es/herramientas-de-mano/c-35-10?q=||relevance||manufacturerNameFacet||Roebuck||categoryLevel3%7C%7C35-10-20",
                "https://es.world.com/es/herramientas-de-mano/c-35-10?q=||relevance||manufacturerNameFacet||Roebuck||categoryLevel3%7C%7C35-10-20"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/search/?text=piquage%20&q=piquage%20||relevance||manufacturerNameFacet||Parker%20Legris||attribute1875%7C%7CM5%20%27%27",
                "https://mydomain.com/search/?q=piquage%20||relevance||manufacturerNameFacet||Parker%20Legris||attribute1875%7C%7CM5%20%27%27"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractContent.aspx?abstractID=9GrIawodOyWmVAXM%2b9Bq3eJFWUiAKhB2Toh3Oct0zIH%2fCbISTIls4l4Ox45ROTAWHCUzXjOonos%3d",
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractContent.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congress/ESC-CONGRESS-2019/Expert-Advice-Tips-and-tricks-in-imaging-your-patient-with-valvular-heart-d/189866-tips-and-tricks-for-imaging-in-aortic-stenosis#video",
                "https://mydomain.com/Congress/ESC-CONGRESS-2019/Expert-Advice-Tips-and-tricks-in-imaging-your-patient-with-valvular-heart-d/189866-tips-and-tricks-for-imaging-in-aortic-stenosis#video"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Councils/Council-on-Hypertension-(CHT)/News/position-statement-of-the-world-council-on-hypertension-on-ace-inhibitors-and-ang",
                "https://mydomain.com/Councils/Council-on-Hypertension-(CHT)/News/position-statement-of-the-world-council-on-hypertension-on-ace-inhibitors-and-ang"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Journals/E-Journal-of-Cardiology-Practice/Volume-14/Treatment-of-right-heart-failure-is-there-a-solution-to-the-problem",
                "https://mydomain.com/Journals/E-Journal-of-Cardiology-Practice/Volume-14/Treatment-of-right-heart-failure-is-there-a-solution-to-the-problem"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congress/EuroCMR-2019/Special-Course-8-how-to-read-CMR-for-extra-cardiac-findings/187189-how-to-interpret-common-organ-specific-findings-in-the-lungs-skeletal-system-liver-kidneys-breast-case-based-interactive-discussion#slide",
                "https://mydomain.com/Congress/EuroCMR-2019/Special-Course-8-how-to-read-CMR-for-extra-cardiac-findings/187189-how-to-interpret-common-organ-specific-findings-in-the-lungs-skeletal-system-liver-kidneys-breast-case-based-interactive-discussion#slide"
        );
        inputUrlToExpectedUrl.put(
                "https://my--domain.force.org/apex/ESCUserProfileInfo",
                "https://my--domain.force.org/apex/ESCUserProfileInfo"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Education/COVID-19-and-Cardiology/ESC-COVID-19-Guidance?hit=home&urlorig=/vgn-ext-templating/#tbl07",
                "https://mydomain.com/Education/COVID-19-and-Cardiology/ESC-COVID-19-Guidance#tbl07"
        );
        inputUrlToExpectedUrl.put(
                "https://my--domain.force.org/ESCMyProfile?utm_medium=Email&utm_source=&utm_campaign=ESC+-+ESC+Congress+2020+-+registration+confirmation#",
                "https://my--domain.force.org/ESCMyProfile#"
        );
        inputUrlToExpectedUrl.put(
                "http://spo.hurence.org/default.aspx?eevtid=1482&showResults=False&_ga=2.267591435.481963044.1578999296-479375258.1578999295",
                "http://spo.hurence.org/default.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congresses-&-Events/Frontiers-in-Cardiovascular-Biomedicine/Registration?utm_medium=Email&utm_source=Councils&utm_campaign=Councils+-+FCVB+2020+-+Early+registration+fee+-++Last+call",
                "https://mydomain.com/Congresses-&-Events/Frontiers-in-Cardiovascular-Biomedicine/Registration"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractAuthors.aspx?abstractID=9GrIawodOyXZLPpXgJHtvCxG5gTt5TznJt97rA1Jy%2bzH7V5eLZVqUnyoo903fiw9nf7mbxKuI14%3d",
                "https://mydomain.com/MyESC/modules/congress/Abstract/AbstractAuthors.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Congresses-&-Events/ESC-Congress/Scientific-sessions",
                "https://mydomain.com/Congresses-&-Events/ESC-Congress/Scientific-sessions"
        );
        inputUrlToExpectedUrl.put(
                "https://idp.hurence.org/idp/login.jsp?loginFailed=true&actionUrl=%2Fidp%2FAuthn%2FESCUserPassword",
                "https://idp.hurence.org/idp/login.jsp"
        );
        inputUrlToExpectedUrl.put(
                "https://aa.net/2016/formulaResult.aspx?model=europelow&exam=&patient=370532",
                "https://aa.net/2016/formulaResult.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://aa.net/login.aspx?ReturnUrl=%2fpercutaneous-interventions%2fhomepage.aspx",
                "https://aa.net/login.aspx"
        );
        inputUrlToExpectedUrl.put(
                "https://mydomain.com/Education/Practice-Tools/EACVI-toolboxes/3D-Echo/Atlas-of-Three%E2%80%93dimensional-Echocardiography/Volumes-and-Ejection-Fraction",
                "https://mydomain.com/Education/Practice-Tools/EACVI-toolboxes/3D-Echo/Atlas-of-Three%E2%80%93dimensional-Echocardiography/Volumes-and-Ejection-Fraction"
        );
        inputUrlToExpectedUrl.put(
                "https://my--domain.force.org/ESCMyPublications",
                "https://my--domain.force.org/ESCMyPublications"
        );
        runTestWithKeepQ(inputUrlToExpectedUrl);
    }
    private void runTestWithRemoveAll(Map<String, String> inputUrlToExpectedUrl) {
        Map<PropertyDescriptor, String> conf = new HashMap<>();
        conf.put(URLCleaner.REMOVE_ALL_PARAMS, "true");
        runTestWithConfig(inputUrlToExpectedUrl, conf);
    }

    private void runTestWithRemoveQ(Map<String, String> inputUrlToExpectedUrl) {
        Map<PropertyDescriptor, String> conf = new HashMap<>();
        conf.put(URLCleaner.REMOVE_PARAMS, "q");
        runTestWithConfig(inputUrlToExpectedUrl, conf);
    }

    private void runTestWithKeepQ(Map<String, String> inputUrlToExpectedUrl) {
        Map<PropertyDescriptor, String> conf = new HashMap<>();
        conf.put(URLCleaner.KEEP_PARAMS, "q");
        runTestWithConfig(inputUrlToExpectedUrl, conf);
    }

    private void runTestWithConfig(Map<String, String> inputUrlToExpectedUrl, Map<PropertyDescriptor, String> conf) {
        List<String> fieldsNames = IntStream.range(1, inputUrlToExpectedUrl.size() + 1)
                .mapToObj(i -> "url" + i)
                .collect(Collectors.toList());
        List<String> inputValues = new ArrayList<>(inputUrlToExpectedUrl.keySet());
        Record record = buildRecordFromMap(fieldsNames, inputValues);
        final Record myCopyOfInitialRecord = new StandardRecord(record);

        TestRunner testRunner = TestRunners.newTestRunner(new URLCleaner());
        testRunner.setProperty(URLCleaner.URL_FIELDS, String.join(",", fieldsNames));
        testRunner.setProperty(URLCleaner.CONFLICT_RESOLUTION_POLICY, URLCleaner.OVERWRITE_EXISTING);
        conf.entrySet().forEach(kv -> {
            testRunner.setProperty(kv.getKey(), kv.getValue());
        });
        testRunner.assertValid();
        testRunner.enqueue(record);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(record.size());

        fieldsNames.forEach(fieldName -> {
            String inputValue = myCopyOfInitialRecord.getField(fieldName).asString();
            String expectedValue = inputUrlToExpectedUrl.get(inputValue);
            out.assertFieldEquals(fieldName, expectedValue);
        });
    }

    private Record buildRecordFromMap(List<String> fieldsNames, List<String> values) {
        if (fieldsNames.size() != values.size()) throw new IllegalArgumentException("list should be of same size");
        final Record record = new StandardRecord();
        IntStream
                .range(0, fieldsNames.size())
                .forEach(i -> {
                    record.setStringField(fieldsNames.get(i), values.get(i));
                });
        return record;
    }
}
