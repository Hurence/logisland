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
package com.hurence.logisland.processor.excel;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.apache.commons.io.IOUtils;
import org.apache.poi.extractor.ExtractorFactory;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

public class ExcelExtractTest {

    private byte[] resolveClassPathResource(String name) throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
            return IOUtils.toByteArray(is);
        }
    }

    private TestRunner initTestRunner(TestRunner testRunner, Integer rowHeaderNumber) {
        if (rowHeaderNumber != null) {
            testRunner.setProperty(ExcelExtractProperties.HEADER_ROW_NB, rowHeaderNumber.toString());
        } else {
            testRunner.setProperty(ExcelExtractProperties.FIELD_NAMES, "Product,Date");
        }
        testRunner.setProperty(ExcelExtractProperties.ROWS_TO_SKIP, "1");
        testRunner.setProperty(ExcelExtractProperties.COLUMNS_TO_SKIP, "0,1,3,4,5,6,7,8,9,10,11");
        return testRunner;
    }

    private void assertRecordValid(Collection<MockRecord> records) {
        records.forEach(record -> {
            record.assertFieldExists("Product");
            record.assertFieldExists("Date");
            record.assertFieldTypeEquals("Product", FieldType.STRING);
            record.assertFieldTypeEquals("Date", FieldType.LONG);
            record.assertFieldExists(Fields.SHEET_NAME);
            record.assertFieldExists(Fields.ROW_NUMBER);
        });
    }

    @Test(expected = AssertionError.class)
    public void testConfigurationValidationErrorWithoutFieldMapping() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new ExcelExtract());
        testRunner.assertValid();
    }

    @Test(expected = AssertionError.class)
    public void testConfigurationValidationErrorWithBothHeaderAndFieldMappingSet() throws Exception {
        final TestRunner testRunner = initTestRunner(TestRunners.newTestRunner(new ExcelExtract()), null);
        testRunner.setProperty(ExcelExtractProperties.HEADER_ROW_NB, "0");
        testRunner.assertValid();
    }

    @Test()
    public void testThrowsExceptionWhenFormatInvalid() throws Exception {
        final TestRunner testRunner = initTestRunner(TestRunners.newTestRunner(new ExcelExtract()), null);
        testRunner.enqueue(FieldDictionary.RECORD_VALUE.getBytes("UTF-8"),
                new String("I'm a fake excel file :)").getBytes("UTF-8"));
        testRunner.run();
        testRunner.assertOutputErrorCount(1);
    }


    @Test
    public void testExtractAllSheets() throws Exception {
        final TestRunner testRunner = initTestRunner(TestRunners.newTestRunner(new ExcelExtract()), null);
        testRunner.enqueue(FieldDictionary.RECORD_VALUE.getBytes("UTF-8"),
                resolveClassPathResource("Financial Sample.xlsx"));
        testRunner.assertValid();
        testRunner.run();
        testRunner.assertOutputRecordsCount(700);
        assertRecordValid(testRunner.getOutputRecords());
    }

    @Test
    public void testExtractNothing() throws Exception {
        final TestRunner testRunner = initTestRunner(TestRunners.newTestRunner(new ExcelExtract()), null);
        testRunner.enqueue(FieldDictionary.RECORD_VALUE.getBytes("UTF-8"),
                resolveClassPathResource("Financial Sample.xlsx"));
        testRunner.setProperty(ExcelExtractProperties.DESIRED_SHEETS, "Sheet2,Sheet3");
        testRunner.assertValid();
        testRunner.run();
        testRunner.assertOutputRecordsCount(0);
    }

    @Test
    public void testExtractSelected() throws Exception {
        final TestRunner testRunner = initTestRunner(TestRunners.newTestRunner(new ExcelExtract()), null);
        testRunner.enqueue(FieldDictionary.RECORD_VALUE.getBytes("UTF-8"),
                resolveClassPathResource("Financial Sample.xlsx"));
        testRunner.setProperty(ExcelExtractProperties.DESIRED_SHEETS, "(?i)sheet.*");
        testRunner.assertValid();
        testRunner.run();
        testRunner.assertOutputRecordsCount(700);
        assertRecordValid(testRunner.getOutputRecords());
    }

    @Test
    public void testExtractWithDynamicMapping() throws Exception {
        final TestRunner testRunner = initTestRunner(TestRunners.newTestRunner(new ExcelExtract()), 0);
        testRunner.enqueue(FieldDictionary.RECORD_VALUE.getBytes("UTF-8"),
                resolveClassPathResource("Financial Sample.xlsx"));
        testRunner.assertValid();
        testRunner.run();
        testRunner.assertOutputRecordsCount(700);
        assertRecordValid(testRunner.getOutputRecords());
    }
}