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

import com.hurence.logisland.processor.util.BaseSyslogTest;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.hurence.logisland.processor.AddFields.DYNAMIC_PROPS_NAME_SUFFIX;
import static com.hurence.logisland.processor.AddFields.DYNAMIC_PROPS_TYPE_SUFFIX;

public class AddFieldsTest extends BaseSyslogTest {

    private static final Logger logger = LoggerFactory.getLogger(AddFieldsTest.class);

    private Record getRecord1() {
        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value2");
        record1.setField("long1", FieldType.LONG, 1);
        record1.setField("long2", FieldType.LONG, 2);
        return record1;
    }

    @Test
    public void testNoMapping() {
        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }

    @Test
    public void testAddValueToExistingField() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty("string1", "NEW");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }

    @Test
    public void testAddValueToExistingField_Overwrite() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty("string1", "NEW");
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
        out.assertFieldEquals("string1", "NEW");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("string2", "value2");
        out.assertFieldTypeEquals("string2", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }

    @Test
    public void testAddNewFields() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty("string3", "value3");
        testRunner.setProperty("string32", "value32");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(6);
        out.assertFieldEquals("string3", "value3");
        out.assertFieldTypeEquals("string3", FieldType.STRING);
        out.assertFieldEquals("string32", "value32");
        out.assertFieldTypeEquals("string32", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }

    @Test
    public void testMultipleFields() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("string1", "NEW");
        testRunner.setProperty("string3", "value3");
        testRunner.setProperty("string32", "value32");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(6);
        out.assertFieldEquals("string1", "NEW");
        out.assertFieldTypeEquals("string1", FieldType.STRING);
        out.assertFieldEquals("string3", "value3");
        out.assertFieldTypeEquals("string3", FieldType.STRING);
        out.assertFieldEquals("string32", "value32");
        out.assertFieldTypeEquals("string32", FieldType.STRING);
        out.assertFieldEquals("long1", 1L);
        out.assertFieldTypeEquals("long1", FieldType.LONG);
        out.assertFieldEquals("long2", 2L);
        out.assertFieldTypeEquals("long2", FieldType.LONG);
    }

    @Test
    public void testAddWithConflictKeepOnlyOld() {

        Record record1 = getRecord1();

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty("string1", "NEW");
        testRunner.setProperty("string2", "NEW");
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.KEEP_ONLY_OLD_FIELD);
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);
        out.assertFieldEquals("string1", "value1");
        out.assertFieldEquals("string2", "value2");
    }


    @Test
    public void testMultipleFieldsWithExpressionLanguage() {

        Record record1 = new StandardRecord();
        record1.setField("string1", FieldType.STRING, "value1");
        record1.setField("string2", FieldType.STRING, "value_2G");
        record1.setField("string3", FieldType.STRING, "value3");

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("stringEL1", "${'string1:'+string1+',string2:'+string2}");
        testRunner.setProperty("stringEL2", "" +
                "${if( string1 == 'value11' || string1 == 'value12') return 'string1'; " +
                "else if ( string2 contains '_' ) return 'string2';}");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("stringEL1", "string1:value1,string2:value_2G");
        out.assertFieldEquals("stringEL2", "string2");
        out.assertFieldTypeEquals("stringEL1", FieldType.STRING);
    }

    @Test
    public void testMultipleFieldsWithComplexExpressionLanguageNonNullValues() {

        Record record1 = new StandardRecord();
        record1.setField("ImportanceCode", FieldType.STRING, "9003");
        record1.setField("ClientID", FieldType.STRING, "12_54");
        record1.setField("libelle_zone", FieldType.STRING, "EST");

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("category", "" +
                "${if( ImportanceCode == '9003' || ImportanceCode == '9004') return 'A'; if ( ClientID contains '_' ) return 'M'; if ( libelle_zone != 'EST' && libelle_zone != 'OUEST' && libelle_zone != 'NORD' ) return 'subsidiaries'; else return 'I';}");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("category", "A");
    }

    @Test
    public void testMultipleFieldsWithComplex2ExpressionLanguageNonNullValues() {

        Record record1 = new StandardRecord();
        record1.setField("ImportanceCode", FieldType.STRING, "9008");
        record1.setField("ClientID", FieldType.STRING, "12*_*54");
        record1.setField("libelle_zone", FieldType.STRING, "EST");

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("category", "" +
                "${if( ImportanceCode == '9003' || ImportanceCode == '9004') return 'A'; if ( ClientID contains '*_*' ) return 'M'; if ( libelle_zone != 'EST' && libelle_zone != 'OUEST' && libelle_zone != 'NORD' ) return 'subsidiaries'; else return 'I';}");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("category", "M");
    }

    @Test
    public void testMultipleFieldsWithComplexExpressionLanguageWithEmptyValues() {

        Record record1 = new StandardRecord();
        record1.setField("ImportantFlag", FieldType.STRING, "");
        record1.setField("ClientID", FieldType.STRING, "");
        record1.setField("libelle_zone", FieldType.STRING, "");

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("category", "" +
                "${if( ImportantFlag == '9003' || ImportantFlag == '9004') return 'A'; if ( ClientID contains '_' ) return 'M'; if ( libelle_zone != 'EST' && libelle_zone != 'OUEST' && libelle_zone != 'NORD' ) return 'subsidiaries'; else return 'I';}");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("category", "subsidiaries");
    }


    @Test
    public void testMultipleFieldsWithComplexExpressionLanguageWithNullValues() {

        Record record1 = new StandardRecord();
        record1.setField("ImportantFlag", FieldType.STRING, null);
        record1.setField("ClientID", FieldType.STRING, null);
        record1.setField("zone", FieldType.STRING, null);

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("category", "" +
                "${if ( ImportantFlag != null && (ImportantFlag == '9003' || ImportantFlag == '9004')) return 'A'; if ( ClientID contains '_' ) return 'M'; if ( zone != 'EST' && zone != 'OUEST' && zone != 'NORD' ) return 'S'; else return 'I';}");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("category", "S");
    }

    @Test
    public void testOldTypeValueIsKept() {

        Record record1 = new StandardRecord();
        record1.setField("number", FieldType.LONG, 1L);

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("number", "156");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("number", 156L);
        out.assertFieldTypeEquals("number", FieldType.LONG);
    }

    @Test
    public void testChoiceOfTypeForValue() {

        Record record1 = new StandardRecord();
        record1.setField("number", FieldType.STRING, "1");

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("number", "156");
        testRunner.setProperty("number" + DYNAMIC_PROPS_TYPE_SUFFIX, "LONG");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("number", 156L);
        out.assertFieldTypeEquals("number", FieldType.LONG);
    }

    @Test
    public void testChoiceOfTypeForValue2() {

        Record record1 = new StandardRecord();
        record1.setField("number", FieldType.STRING, "1");

        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("number", "156");
        testRunner.setProperty("number" + DYNAMIC_PROPS_TYPE_SUFFIX, "long");
        testRunner.assertNotValid();
    }

    @Test
    public void testDocExample() {

        Record record1 = new StandardRecord();
        record1.setField("field1", FieldType.STRING, "value1");
        record1.setField("field2", FieldType.STRING, "value2");
        int sizeBeforeProcessing = record1.size();
        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("concat2fields", "${field1 + \"_\" + field2}");
        testRunner.setProperty("my_countries", "${[\"france\", \"allemagne\"]}");
        testRunner.setProperty("my_countries" + DYNAMIC_PROPS_TYPE_SUFFIX, "ARRAY");
        testRunner.setProperty("my_employees_by_countries", "${[\"france\" : 100, \"allemagne\" : 50]}");
        testRunner.setProperty("my_employees_by_countries" + DYNAMIC_PROPS_TYPE_SUFFIX, "MAP");
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("concat2fields", "value1_value2");
        out.assertFieldTypeEquals("concat2fields", FieldType.STRING);
        final ArrayList<String> expectedArray = new ArrayList<>();
        expectedArray.add("france");
        expectedArray.add("allemagne");
        out.assertFieldEquals("my_countries", expectedArray);
        out.assertFieldTypeEquals("my_countries", FieldType.ARRAY);
        final Map<String, Integer> expectedmap = new HashMap<>();
        expectedmap.put("france", 100);
        expectedmap.put("allemagne", 50);
        out.assertFieldEquals("my_employees_by_countries", expectedmap);
        out.assertFieldTypeEquals("my_employees_by_countries", FieldType.MAP);
        out.assertRecordSizeEquals(sizeBeforeProcessing + 3);//should only have 3 more fields
    }

    @Test
    public void testWithEexpressionLanguageInName() {

        Record record1 = new StandardRecord();
        record1.setField("field1", FieldType.STRING, "value1");
        record1.setField("field2", FieldType.STRING, "value2");
        int sizeBeforeProcessing = record1.size();
        TestRunner testRunner = TestRunners.newTestRunner(new AddFields());
        testRunner.setProperty(NormalizeFields.CONFLICT_RESOLUTION_POLICY, NormalizeFields.OVERWRITE_EXISTING);
        testRunner.setProperty("caulcalOfA", "${field1 + \"_\" + field2}");
        testRunner.setProperty("caulcalOfA" + DYNAMIC_PROPS_NAME_SUFFIX, "${\"_\" + field1 + \"_\"}");
        testRunner.setProperty("caulcalOfA" + DYNAMIC_PROPS_TYPE_SUFFIX, FieldType.STRING.getName().toUpperCase());
        testRunner.assertValid();
        testRunner.enqueue(record1);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldNotExists("caulcalOfA");
        out.assertFieldEquals("_value1_", "value1_value2");
        out.assertFieldTypeEquals("_value1_", FieldType.STRING);
        out.assertRecordSizeEquals(sizeBeforeProcessing + 1);//should only have 3 more fields
    }
}
