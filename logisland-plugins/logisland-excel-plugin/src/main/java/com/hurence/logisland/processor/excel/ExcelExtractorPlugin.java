/*
 * Copyright (C) 2018 Hurence (support@hurence.com)
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
 *
 */
package com.hurence.logisland.processor.excel;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.*;
import org.apache.commons.io.IOUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.NotOfficeXmlFileException;
import org.apache.poi.ss.usermodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Consumes a Microsoft Excel document and converts each spreadsheet row to a {@link Record}.
 */
@Tags({"excel", "processor", "poi"})
@CapabilityDescription("Consumes a Microsoft Excel document and converts each worksheet's line to a structured " +
        "record. The processor is assuming to receive raw excel file as input record.")
public class ExcelExtractorPlugin extends AbstractProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExcelExtractorProperties.class);

    /**
     * The configuration.
     */
    private ExcelExtractorProperties.Configuration configuration;


    @Override
    public void init(ProcessContext context) {
        super.init(context);
        configuration = new ExcelExtractorProperties.Configuration(context);
        LOGGER.info("ExcelExtractorPlugin successfully initialized");
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ExcelExtractorProperties.DESIRED_SHEETS);
        descriptors.add(ExcelExtractorProperties.COLUMNS_TO_SKIP);
        descriptors.add(ExcelExtractorProperties.FIELD_NAMES);
        descriptors.add(ExcelExtractorProperties.ROWS_TO_SKIP);
        descriptors.add(ExcelExtractorProperties.RECORD_TYPE);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        final Collection<Record> ret = new ArrayList<>();
        for (Record record : records) {
            //Extract source input stream
            InputStream is = extractRawContent(record);
            //process
            ret.addAll(handleExcelStream(is)
                    //enrich
                    .map(current -> enrichWithMetadata(current, record))
                    //collect and add to global results
                    .collect(Collectors.toList()));
        }
        return ret;
    }


    private final Record enrichWithMetadata(Record current, Record source) {
        if (source.hasField(Fields.SOURCE_FILE_NAME)) {
            current.setField(source.getField(Fields.SOURCE_FILE_NAME));
        }
        current.setField(Fields.recordType(configuration.getRecordType()));
        return current;
    }


    /**
     * Extract the raw byte XLS content from the input record.
     *
     * @param record
     * @return A byte array inputstream (never null).
     * @throws IllegalStateException in case of malformed record.
     */
    private InputStream extractRawContent(Record record) {
        if (!record.hasField(FieldDictionary.RECORD_VALUE)) {
            throw new IllegalStateException("Received a record not carrying information on field " + FieldDictionary.RECORD_VALUE);
        }
        Field field = record.getField(FieldDictionary.RECORD_VALUE);
        if (field == null || !FieldType.BYTES.equals(field.getType())) {
            throw new IllegalStateException("Unexpected content received. We expect to handle field content with raw byte data.");
        }
        return new ByteArrayInputStream((byte[]) field.getRawValue());
    }

    /**
     * Extract every matching sheet from the raw excel input stream.
     *
     * @param inputStream an inputstream that will be closed once consumed.
     * @return a stream of {@link Record} each containing the stream raw data.
     */
    private Stream<Record> handleExcelStream(InputStream inputStream) {
        List<Record> ret = new ArrayList<>();
        try {
            try (Workbook workbook = WorkbookFactory.create(inputStream)) {
                Iterator<Sheet> iter = workbook.sheetIterator();
                while (iter.hasNext()) {
                    try {
                        Sheet sheet = iter.next();
                        String sheetName = sheet.getSheetName();
                        if (toBeSkipped(sheetName)) {
                            LOGGER.info("Skipped sheet {}", sheetName);
                            continue;
                        }
                        LOGGER.info("Extracting sheet {}", sheetName);
                        int count = 0;
                        for (Row row : sheet) {
                            if (row == null) {
                                continue;
                            }
                            if (count++ < configuration.getRowsToSkip()) {
                                continue;
                            }
                            ret.add(handleRow(row));
                        }

                    } catch (Exception e) {
                        LOGGER.error("Unrecoverable exception occurred while processing excel sheet", e);
                    }
                }
            }
        } catch (InvalidFormatException | NotOfficeXmlFileException ife) {
            throw new UnsupportedOperationException("Wrong or unsupported file format.", ife);
        } catch (IOException ioe) {
            LOGGER.error("I/O Exception occurred while processing excel file", ioe);

        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return ret.stream();
    }


    /**
     * Handle row content and transform it into a {@link Record}
     *
     * @param row the {@link Row}
     * @return the transformed {@link Record}
     */
    private Record handleRow(Row row) {
        Record ret = new StandardRecord().setTime(new Date());
        int index = 0;
        for (Cell cell : row) {
            if (configuration.getColumnsToSkip().contains(cell.getColumnIndex()) || index >= configuration.getFieldNames().size()) {
                //skip this cell.
                continue;
            }
            String fieldName = configuration.getFieldNames().get(index++);
            Field field;
            // Alternatively, get the value and format it yourself
            switch (cell.getCellTypeEnum()) {
                case STRING:
                    field = new Field(fieldName, FieldType.STRING, cell.getStringCellValue());
                    break;
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        field = new Field(fieldName, FieldType.LONG, cell.getDateCellValue().getTime());
                    } else {
                        field = new Field(fieldName, FieldType.DOUBLE, cell.getNumericCellValue());
                    }
                    break;
                case BOOLEAN:
                    field = new Field(fieldName, FieldType.BOOLEAN, cell.getBooleanCellValue());
                    break;
                case FORMULA:
                    field = new Field(fieldName, FieldType.STRING, cell.getCellFormula());
                    break;
                default:
                    //blank or unknown
                    field = new Field(fieldName, FieldType.NULL, null);
                    break;
            }
            ret.setField(field);
        }
        return ret;
    }


    /**
     * Looks if a sheet should be extracted or not according to the configuration.
     *
     * @param sheet the name of the sheet
     * @return true if the current sheet has to be skipped. False otherwise.
     */
    private boolean toBeSkipped(String sheet) {
        for (Pattern pattern : configuration.getSheetsToExtract()) {
            if (pattern.matcher(sheet).matches()) {
                return false;
            }
        }
        return !configuration.getSheetsToExtract().isEmpty();
    }


}
