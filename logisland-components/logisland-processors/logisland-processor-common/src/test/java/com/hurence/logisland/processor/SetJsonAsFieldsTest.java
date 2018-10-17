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
package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SetJsonAsFieldsTest {
    
    private static Logger logger = LoggerFactory.getLogger(SetJsonAsFieldsTest.class);
    
    // Bro conn input event
    private static final String SIMPLE_JSON =
            "{" +
                    "\"nullAttribute\": null," +
                    "\"emptyString\": \"\"," +
                    "\"attributeString1\": \"attributeString1Value\"," +
                    "\"attributeString2\": \"attributeString2Value\"," +
                    "\"attributeInt1\": 9," +
                    "\"attributeInt2\": 7800," +
                    "\"attributeFloat1\": 123.456," +
                    "\"attributeFloat2\": 12.12345," +
                    "\"attributeDouble1\": 1235234567.3215," +
                    "\"attributeDouble2\": 8259434578.32265415," +
                    "\"attributeLong1\": 32345678910," +
                    "\"attributeLong2\": 25643297851," +
                    "\"attributeBoolean1\": true," +
                    "\"attributeBoolean2\": false," +
                    "\"attributeListString\": [\"attributeListStringValue1\",\"attributeListStringValue2\"]," +
                    "\"attributeListInt\": [1123, 7456]," +
                    "\"attributeMap\":  { \"attributeMapValueString\": \"attributeMapValueStringValue\"," +
                                         "\"attributeMapValueInt\": 2456," +
                                         "\"attributeMapValueBoolean\": false }" +
            "}";
    
    /**
     * Test with simple document in record_value field
     */
    @Test
    public void testSimpleJson() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SetJsonAsFields());
        testRunner.assertValid();
        Record record = new StandardRecord("json_event");
        record.setStringField(FieldDictionary.RECORD_VALUE, SIMPLE_JSON);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        
        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "json_event");

        out.assertFieldExists("nullAttribute");
        out.assertNullField("nullAttribute");

        out.assertFieldExists("emptyString");
        out.assertFieldEquals("emptyString", "");
        
        out.assertFieldExists("attributeString1");
        out.assertFieldEquals("attributeString1", "attributeString1Value");

        out.assertFieldExists("attributeString2");
        out.assertFieldEquals("attributeString2", "attributeString2Value");

        out.assertFieldExists("attributeInt1");
        out.assertFieldEquals("attributeInt1", 9);

        out.assertFieldExists("attributeInt2");
        out.assertFieldEquals("attributeInt2", 7800);

        out.assertFieldExists("attributeBoolean1");
        out.assertFieldEquals("attributeBoolean1", true);

        out.assertFieldExists("attributeBoolean2");
        out.assertFieldEquals("attributeBoolean2", false);

        out.assertFieldExists("attributeFloat1");
        out.assertFieldEquals("attributeFloat1", (float)123.456);

        out.assertFieldExists("attributeFloat2");
        out.assertFieldEquals("attributeFloat2", (float)12.12345);

        out.assertFieldExists("attributeDouble1");
        out.assertFieldEquals("attributeDouble1", (double)1235234567.3215);

        out.assertFieldExists("attributeDouble2");
        out.assertFieldEquals("attributeDouble2", (double)8259434578.32265415);

        out.assertFieldExists("attributeLong1");
        out.assertFieldEquals("attributeLong1", (long)32345678910L);

        out.assertFieldExists("attributeLong2");
        out.assertFieldEquals("attributeLong2", (long)25643297851L);

        out.assertFieldExists("attributeListString");
        List<String> attributeListString = (List<String>)out.getField("attributeListString").getRawValue();
        assertEquals(Arrays.asList("attributeListStringValue1", "attributeListStringValue2"), attributeListString);

        out.assertFieldExists("attributeListInt");
        List<Integer> attributeListInt = (List<Integer>)out.getField("attributeListInt").getRawValue();
        assertEquals(Arrays.asList(1123, 7456), attributeListInt);

        out.assertFieldExists("attributeMap");
        Map<String, Object> attributeMap = (Map<String, Object>)out.getField("attributeMap").getRawValue();
        String attributeMapValueString = (String)attributeMap.get("attributeMapValueString");
        assertEquals("attributeMapValueStringValue", attributeMapValueString);
        Integer attributeMapValueInt = (Integer)attributeMap.get("attributeMapValueInt");
        assertEquals(new Integer(2456), attributeMapValueInt);
        Boolean attributeMapValueBoolean = (Boolean)attributeMap.get("attributeMapValueBoolean");
        assertEquals(false, attributeMapValueBoolean);

        out.assertFieldNotExists(FieldDictionary.RECORD_VALUE);
    }

    /**
     * Test with simple document in record_value field, remove original json field
     */
    @Test
    public void testSimpleJsonRemoveJson() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SetJsonAsFields());
        testRunner.assertValid();
        Record record = new StandardRecord("json_event");
        record.setStringField(FieldDictionary.RECORD_VALUE, SIMPLE_JSON);
        testRunner.setProperty(SetJsonAsFields.KEEP_JSON_FIELD, "true");
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "json_event");

        out.assertFieldExists("nullAttribute");
        out.assertNullField("nullAttribute");

        out.assertFieldExists("emptyString");
        out.assertFieldEquals("emptyString", "");

        out.assertFieldExists("attributeString1");
        out.assertFieldEquals("attributeString1", "attributeString1Value");

        out.assertFieldExists("attributeString2");
        out.assertFieldEquals("attributeString2", "attributeString2Value");

        out.assertFieldExists("attributeInt1");
        out.assertFieldEquals("attributeInt1", 9);

        out.assertFieldExists("attributeInt2");
        out.assertFieldEquals("attributeInt2", 7800);

        out.assertFieldExists("attributeBoolean1");
        out.assertFieldEquals("attributeBoolean1", true);

        out.assertFieldExists("attributeBoolean2");
        out.assertFieldEquals("attributeBoolean2", false);

        out.assertFieldExists("attributeFloat1");
        out.assertFieldEquals("attributeFloat1", (float)123.456);

        out.assertFieldExists("attributeFloat2");
        out.assertFieldEquals("attributeFloat2", (float)12.12345);

        out.assertFieldExists("attributeDouble1");
        out.assertFieldEquals("attributeDouble1", (double)1235234567.3215);

        out.assertFieldExists("attributeDouble2");
        out.assertFieldEquals("attributeDouble2", (double)8259434578.32265415);

        out.assertFieldExists("attributeLong1");
        out.assertFieldEquals("attributeLong1", (long)32345678910L);

        out.assertFieldExists("attributeLong2");
        out.assertFieldEquals("attributeLong2", (long)25643297851L);

        out.assertFieldExists("attributeListString");
        List<String> attributeListString = (List<String>)out.getField("attributeListString").getRawValue();
        assertEquals(Arrays.asList("attributeListStringValue1", "attributeListStringValue2"), attributeListString);

        out.assertFieldExists("attributeListInt");
        List<Integer> attributeListInt = (List<Integer>)out.getField("attributeListInt").getRawValue();
        assertEquals(Arrays.asList(1123, 7456), attributeListInt);

        out.assertFieldExists("attributeMap");
        Map<String, Object> attributeMap = (Map<String, Object>)out.getField("attributeMap").getRawValue();
        String attributeMapValueString = (String)attributeMap.get("attributeMapValueString");
        assertEquals("attributeMapValueStringValue", attributeMapValueString);
        Integer attributeMapValueInt = (Integer)attributeMap.get("attributeMapValueInt");
        assertEquals(new Integer(2456), attributeMapValueInt);
        Boolean attributeMapValueBoolean = (Boolean)attributeMap.get("attributeMapValueBoolean");
        assertEquals(false, attributeMapValueBoolean);

        out.assertFieldExists(FieldDictionary.RECORD_VALUE);
    }

    /**
     * Test with simple document in custom field
     */
    @Test
    public void testSimpleJsonCustomField() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SetJsonAsFields());
        testRunner.setProperty(SetJsonAsFields.JSON_FIELD, "customField");
        testRunner.assertValid();
        Record record = new StandardRecord("json_event");
        record.setStringField("customField", SIMPLE_JSON);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "json_event");

        out.assertFieldExists("nullAttribute");
        out.assertNullField("nullAttribute");

        out.assertFieldExists("emptyString");
        out.assertFieldEquals("emptyString", "");

        out.assertFieldExists("attributeString1");
        out.assertFieldEquals("attributeString1", "attributeString1Value");

        out.assertFieldExists("attributeString2");
        out.assertFieldEquals("attributeString2", "attributeString2Value");

        out.assertFieldExists("attributeInt1");
        out.assertFieldEquals("attributeInt1", 9);

        out.assertFieldExists("attributeInt2");
        out.assertFieldEquals("attributeInt2", 7800);

        out.assertFieldExists("attributeBoolean1");
        out.assertFieldEquals("attributeBoolean1", true);

        out.assertFieldExists("attributeBoolean2");
        out.assertFieldEquals("attributeBoolean2", false);

        out.assertFieldExists("attributeFloat1");
        out.assertFieldEquals("attributeFloat1", (float)123.456);

        out.assertFieldExists("attributeFloat2");
        out.assertFieldEquals("attributeFloat2", (float)12.12345);

        out.assertFieldExists("attributeDouble1");
        out.assertFieldEquals("attributeDouble1", (double)1235234567.3215);

        out.assertFieldExists("attributeDouble2");
        out.assertFieldEquals("attributeDouble2", (double)8259434578.32265415);

        out.assertFieldExists("attributeLong1");
        out.assertFieldEquals("attributeLong1", (long)32345678910L);

        out.assertFieldExists("attributeLong2");
        out.assertFieldEquals("attributeLong2", (long)25643297851L);

        out.assertFieldExists("attributeListString");
        List<String> attributeListString = (List<String>)out.getField("attributeListString").getRawValue();
        assertEquals(Arrays.asList("attributeListStringValue1", "attributeListStringValue2"), attributeListString);

        out.assertFieldExists("attributeListInt");
        List<Integer> attributeListInt = (List<Integer>)out.getField("attributeListInt").getRawValue();
        assertEquals(Arrays.asList(1123, 7456), attributeListInt);

        out.assertFieldExists("attributeMap");
        Map<String, Object> attributeMap = (Map<String, Object>)out.getField("attributeMap").getRawValue();
        String attributeMapValueString = (String)attributeMap.get("attributeMapValueString");
        assertEquals("attributeMapValueStringValue", attributeMapValueString);
        Integer attributeMapValueInt = (Integer)attributeMap.get("attributeMapValueInt");
        assertEquals(new Integer(2456), attributeMapValueInt);
        Boolean attributeMapValueBoolean = (Boolean)attributeMap.get("attributeMapValueBoolean");
        assertEquals(false, attributeMapValueBoolean);

        out.assertFieldNotExists(FieldDictionary.RECORD_VALUE);
    }

    /**
     * Test with simple document in custom field, remove original json field
     */
    @Test
    public void testSimpleJsonCustomFieldRemoveJson() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SetJsonAsFields());
        testRunner.setProperty(SetJsonAsFields.JSON_FIELD, "customField");
        testRunner.setProperty(SetJsonAsFields.KEEP_JSON_FIELD, "true");
        testRunner.assertValid();
        Record record = new StandardRecord("json_event");
        record.setStringField("customField", SIMPLE_JSON);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "json_event");

        out.assertFieldExists("nullAttribute");
        out.assertNullField("nullAttribute");

        out.assertFieldExists("emptyString");
        out.assertFieldEquals("emptyString", "");

        out.assertFieldExists("attributeString1");
        out.assertFieldEquals("attributeString1", "attributeString1Value");

        out.assertFieldExists("attributeString2");
        out.assertFieldEquals("attributeString2", "attributeString2Value");

        out.assertFieldExists("attributeInt1");
        out.assertFieldEquals("attributeInt1", 9);

        out.assertFieldExists("attributeInt2");
        out.assertFieldEquals("attributeInt2", 7800);

        out.assertFieldExists("attributeBoolean1");
        out.assertFieldEquals("attributeBoolean1", true);

        out.assertFieldExists("attributeBoolean2");
        out.assertFieldEquals("attributeBoolean2", false);

        out.assertFieldExists("attributeFloat1");
        out.assertFieldEquals("attributeFloat1", (float)123.456);

        out.assertFieldExists("attributeFloat2");
        out.assertFieldEquals("attributeFloat2", (float)12.12345);

        out.assertFieldExists("attributeDouble1");
        out.assertFieldEquals("attributeDouble1", (double)1235234567.3215);

        out.assertFieldExists("attributeDouble2");
        out.assertFieldEquals("attributeDouble2", (double)8259434578.32265415);

        out.assertFieldExists("attributeLong1");
        out.assertFieldEquals("attributeLong1", (long)32345678910L);

        out.assertFieldExists("attributeLong2");
        out.assertFieldEquals("attributeLong2", (long)25643297851L);

        out.assertFieldExists("attributeListString");
        List<String> attributeListString = (List<String>)out.getField("attributeListString").getRawValue();
        assertEquals(Arrays.asList("attributeListStringValue1", "attributeListStringValue2"), attributeListString);

        out.assertFieldExists("attributeListInt");
        List<Integer> attributeListInt = (List<Integer>)out.getField("attributeListInt").getRawValue();
        assertEquals(Arrays.asList(1123, 7456), attributeListInt);

        out.assertFieldExists("attributeMap");
        Map<String, Object> attributeMap = (Map<String, Object>)out.getField("attributeMap").getRawValue();
        String attributeMapValueString = (String)attributeMap.get("attributeMapValueString");
        assertEquals("attributeMapValueStringValue", attributeMapValueString);
        Integer attributeMapValueInt = (Integer)attributeMap.get("attributeMapValueInt");
        assertEquals(new Integer(2456), attributeMapValueInt);
        Boolean attributeMapValueBoolean = (Boolean)attributeMap.get("attributeMapValueBoolean");
        assertEquals(false, attributeMapValueBoolean);

        out.assertFieldExists("customField");
    }

    /**
     * Test with simple document in record_value field and an existing field to overwrite
     */
    @Test
    public void testSimpleJsonOverwrite() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SetJsonAsFields());
        testRunner.assertValid();
        Record record = new StandardRecord("json_event");
        record.setStringField(FieldDictionary.RECORD_VALUE, SIMPLE_JSON);
        record.setStringField("attributeString1", "existingValueToOverwrite");
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "json_event");

        out.assertFieldExists("nullAttribute");
        out.assertNullField("nullAttribute");

        out.assertFieldExists("emptyString");
        out.assertFieldEquals("emptyString", "");

        out.assertFieldExists("attributeString1");
        out.assertFieldEquals("attributeString1", "attributeString1Value");

        out.assertFieldExists("attributeString2");
        out.assertFieldEquals("attributeString2", "attributeString2Value");

        out.assertFieldExists("attributeInt1");
        out.assertFieldEquals("attributeInt1", 9);

        out.assertFieldExists("attributeInt2");
        out.assertFieldEquals("attributeInt2", 7800);

        out.assertFieldExists("attributeBoolean1");
        out.assertFieldEquals("attributeBoolean1", true);

        out.assertFieldExists("attributeBoolean2");
        out.assertFieldEquals("attributeBoolean2", false);

        out.assertFieldExists("attributeFloat1");
        out.assertFieldEquals("attributeFloat1", (float)123.456);

        out.assertFieldExists("attributeFloat2");
        out.assertFieldEquals("attributeFloat2", (float)12.12345);

        out.assertFieldExists("attributeDouble1");
        out.assertFieldEquals("attributeDouble1", (double)1235234567.3215);

        out.assertFieldExists("attributeDouble2");
        out.assertFieldEquals("attributeDouble2", (double)8259434578.32265415);

        out.assertFieldExists("attributeLong1");
        out.assertFieldEquals("attributeLong1", (long)32345678910L);

        out.assertFieldExists("attributeLong2");
        out.assertFieldEquals("attributeLong2", (long)25643297851L);

        out.assertFieldExists("attributeListString");
        List<String> attributeListString = (List<String>)out.getField("attributeListString").getRawValue();
        assertEquals(Arrays.asList("attributeListStringValue1", "attributeListStringValue2"), attributeListString);

        out.assertFieldExists("attributeListInt");
        List<Integer> attributeListInt = (List<Integer>)out.getField("attributeListInt").getRawValue();
        assertEquals(Arrays.asList(1123, 7456), attributeListInt);

        out.assertFieldExists("attributeMap");
        Map<String, Object> attributeMap = (Map<String, Object>)out.getField("attributeMap").getRawValue();
        String attributeMapValueString = (String)attributeMap.get("attributeMapValueString");
        assertEquals("attributeMapValueStringValue", attributeMapValueString);
        Integer attributeMapValueInt = (Integer)attributeMap.get("attributeMapValueInt");
        assertEquals(new Integer(2456), attributeMapValueInt);
        Boolean attributeMapValueBoolean = (Boolean)attributeMap.get("attributeMapValueBoolean");
        assertEquals(false, attributeMapValueBoolean);

        out.assertFieldNotExists(FieldDictionary.RECORD_VALUE);
    }

    /**
     * Test with simple document in record_value field and an existing field to not overwrite
     */
    @Test
    public void testSimpleJsonNoOverwrite() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SetJsonAsFields());
        testRunner.setProperty(SetJsonAsFields.OVERWRITE_EXISTING_FIELD, "false");
        testRunner.assertValid();
        Record record = new StandardRecord("json_event");
        record.setStringField(FieldDictionary.RECORD_VALUE, SIMPLE_JSON);
        record.setStringField("attributeString1", "existingValueToNotOverwrite");
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "json_event");

        out.assertFieldExists("nullAttribute");
        out.assertNullField("nullAttribute");

        out.assertFieldExists("emptyString");
        out.assertFieldEquals("emptyString", "");

        out.assertFieldExists("attributeString1");
        out.assertFieldEquals("attributeString1", "existingValueToNotOverwrite"); // Should have not been overwritten

        out.assertFieldExists("attributeString2");
        out.assertFieldEquals("attributeString2", "attributeString2Value");

        out.assertFieldExists("attributeInt1");
        out.assertFieldEquals("attributeInt1", 9);

        out.assertFieldExists("attributeInt2");
        out.assertFieldEquals("attributeInt2", 7800);

        out.assertFieldExists("attributeBoolean1");
        out.assertFieldEquals("attributeBoolean1", true);

        out.assertFieldExists("attributeBoolean2");
        out.assertFieldEquals("attributeBoolean2", false);

        out.assertFieldExists("attributeFloat1");
        out.assertFieldEquals("attributeFloat1", (float)123.456);

        out.assertFieldExists("attributeFloat2");
        out.assertFieldEquals("attributeFloat2", (float)12.12345);

        out.assertFieldExists("attributeDouble1");
        out.assertFieldEquals("attributeDouble1", (double)1235234567.3215);

        out.assertFieldExists("attributeDouble2");
        out.assertFieldEquals("attributeDouble2", (double)8259434578.32265415);

        out.assertFieldExists("attributeLong1");
        out.assertFieldEquals("attributeLong1", (long)32345678910L);

        out.assertFieldExists("attributeLong2");
        out.assertFieldEquals("attributeLong2", (long)25643297851L);

        out.assertFieldExists("attributeListString");
        List<String> attributeListString = (List<String>)out.getField("attributeListString").getRawValue();
        assertEquals(Arrays.asList("attributeListStringValue1", "attributeListStringValue2"), attributeListString);

        out.assertFieldExists("attributeListInt");
        List<Integer> attributeListInt = (List<Integer>)out.getField("attributeListInt").getRawValue();
        assertEquals(Arrays.asList(1123, 7456), attributeListInt);

        out.assertFieldExists("attributeMap");
        Map<String, Object> attributeMap = (Map<String, Object>)out.getField("attributeMap").getRawValue();
        String attributeMapValueString = (String)attributeMap.get("attributeMapValueString");
        assertEquals("attributeMapValueStringValue", attributeMapValueString);
        Integer attributeMapValueInt = (Integer)attributeMap.get("attributeMapValueInt");
        assertEquals(new Integer(2456), attributeMapValueInt);
        Boolean attributeMapValueBoolean = (Boolean)attributeMap.get("attributeMapValueBoolean");
        assertEquals(false, attributeMapValueBoolean);

        out.assertFieldNotExists(FieldDictionary.RECORD_VALUE);
    }

    /**
     * Test with simple document in record_value field, omitting null attributes
     */
    @Test
    public void testSimpleJsonOmitNullAttributes() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SetJsonAsFields());
        testRunner.setProperty(SetJsonAsFields.OMIT_NULL_ATTRIBUTES, "true");
        testRunner.assertValid();
        Record record = new StandardRecord("json_event");
        record.setStringField(FieldDictionary.RECORD_VALUE, SIMPLE_JSON);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "json_event");

        out.assertFieldNotExists("nullAttribute");

        out.assertFieldExists("emptyString");
        out.assertFieldEquals("emptyString", "");

        out.assertFieldExists("attributeString1");
        out.assertFieldEquals("attributeString1", "attributeString1Value");

        out.assertFieldExists("attributeString2");
        out.assertFieldEquals("attributeString2", "attributeString2Value");

        out.assertFieldExists("attributeInt1");
        out.assertFieldEquals("attributeInt1", 9);

        out.assertFieldExists("attributeInt2");
        out.assertFieldEquals("attributeInt2", 7800);

        out.assertFieldExists("attributeBoolean1");
        out.assertFieldEquals("attributeBoolean1", true);

        out.assertFieldExists("attributeBoolean2");
        out.assertFieldEquals("attributeBoolean2", false);

        out.assertFieldExists("attributeFloat1");
        out.assertFieldEquals("attributeFloat1", (float)123.456);

        out.assertFieldExists("attributeFloat2");
        out.assertFieldEquals("attributeFloat2", (float)12.12345);

        out.assertFieldExists("attributeDouble1");
        out.assertFieldEquals("attributeDouble1", (double)1235234567.3215);

        out.assertFieldExists("attributeDouble2");
        out.assertFieldEquals("attributeDouble2", (double)8259434578.32265415);

        out.assertFieldExists("attributeLong1");
        out.assertFieldEquals("attributeLong1", (long)32345678910L);

        out.assertFieldExists("attributeLong2");
        out.assertFieldEquals("attributeLong2", (long)25643297851L);

        out.assertFieldExists("attributeListString");
        List<String> attributeListString = (List<String>)out.getField("attributeListString").getRawValue();
        assertEquals(Arrays.asList("attributeListStringValue1", "attributeListStringValue2"), attributeListString);

        out.assertFieldExists("attributeListInt");
        List<Integer> attributeListInt = (List<Integer>)out.getField("attributeListInt").getRawValue();
        assertEquals(Arrays.asList(1123, 7456), attributeListInt);

        out.assertFieldExists("attributeMap");
        Map<String, Object> attributeMap = (Map<String, Object>)out.getField("attributeMap").getRawValue();
        String attributeMapValueString = (String)attributeMap.get("attributeMapValueString");
        assertEquals("attributeMapValueStringValue", attributeMapValueString);
        Integer attributeMapValueInt = (Integer)attributeMap.get("attributeMapValueInt");
        assertEquals(new Integer(2456), attributeMapValueInt);
        Boolean attributeMapValueBoolean = (Boolean)attributeMap.get("attributeMapValueBoolean");
        assertEquals(false, attributeMapValueBoolean);

        out.assertFieldNotExists(FieldDictionary.RECORD_VALUE);
    }

    /**
     * Test with simple document in record_value field, omitting empty string attributes
     */
    @Test
    public void testSimpleJsonOmitEmptyStringAttributes() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SetJsonAsFields());
        testRunner.setProperty(SetJsonAsFields.OMIT_EMPTY_STRING_ATTRIBUTES, "true");
        testRunner.assertValid();
        Record record = new StandardRecord("json_event");
        record.setStringField(FieldDictionary.RECORD_VALUE, SIMPLE_JSON);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.RECORD_TYPE);
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "json_event");

        out.assertFieldExists("nullAttribute");
        out.assertNullField("nullAttribute");

        out.assertFieldNotExists("emptyString");

        out.assertFieldExists("attributeString1");
        out.assertFieldEquals("attributeString1", "attributeString1Value");

        out.assertFieldExists("attributeString2");
        out.assertFieldEquals("attributeString2", "attributeString2Value");

        out.assertFieldExists("attributeInt1");
        out.assertFieldEquals("attributeInt1", 9);

        out.assertFieldExists("attributeInt2");
        out.assertFieldEquals("attributeInt2", 7800);

        out.assertFieldExists("attributeBoolean1");
        out.assertFieldEquals("attributeBoolean1", true);

        out.assertFieldExists("attributeBoolean2");
        out.assertFieldEquals("attributeBoolean2", false);

        out.assertFieldExists("attributeFloat1");
        out.assertFieldEquals("attributeFloat1", (float)123.456);

        out.assertFieldExists("attributeFloat2");
        out.assertFieldEquals("attributeFloat2", (float)12.12345);

        out.assertFieldExists("attributeDouble1");
        out.assertFieldEquals("attributeDouble1", (double)1235234567.3215);

        out.assertFieldExists("attributeDouble2");
        out.assertFieldEquals("attributeDouble2", (double)8259434578.32265415);

        out.assertFieldExists("attributeLong1");
        out.assertFieldEquals("attributeLong1", (long)32345678910L);

        out.assertFieldExists("attributeLong2");
        out.assertFieldEquals("attributeLong2", (long)25643297851L);

        out.assertFieldExists("attributeListString");
        List<String> attributeListString = (List<String>)out.getField("attributeListString").getRawValue();
        assertEquals(Arrays.asList("attributeListStringValue1", "attributeListStringValue2"), attributeListString);

        out.assertFieldExists("attributeListInt");
        List<Integer> attributeListInt = (List<Integer>)out.getField("attributeListInt").getRawValue();
        assertEquals(Arrays.asList(1123, 7456), attributeListInt);

        out.assertFieldExists("attributeMap");
        Map<String, Object> attributeMap = (Map<String, Object>)out.getField("attributeMap").getRawValue();
        String attributeMapValueString = (String)attributeMap.get("attributeMapValueString");
        assertEquals("attributeMapValueStringValue", attributeMapValueString);
        Integer attributeMapValueInt = (Integer)attributeMap.get("attributeMapValueInt");
        assertEquals(new Integer(2456), attributeMapValueInt);
        Boolean attributeMapValueBoolean = (Boolean)attributeMap.get("attributeMapValueBoolean");
        assertEquals(false, attributeMapValueBoolean);

        out.assertFieldNotExists(FieldDictionary.RECORD_VALUE);
    }
}
