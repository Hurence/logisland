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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.*;
import com.hurence.logisland.util.stream.io.StreamUtils;
import com.hurence.logisland.validator.Configuration;
import com.hurence.logisland.validator.ValidationResult;
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
@ExtraDetailFile("./details/ExcelExtract-Detail.rst")
public class ExcelExtract extends AbstractProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExcelExtractProperties.class);

    /**
     * The configuration.
     */
    private ExcelExtractProperties.Configuration configuration;


    @Override
    public void init(ProcessContext context) throws InitializationException {
        super.init(context);
        configuration = new ExcelExtractProperties.Configuration(context);
        LOGGER.info("ExcelExtract successfully initialized");
    }


    @Override
    protected Collection<ValidationResult> customValidate(Configuration context) {
        ValidationResult.Builder ret = new ValidationResult.Builder().valid(true);
        if (!(context.getPropertyValue(ExcelExtractProperties.FIELD_NAMES).isSet() ^
                context.getPropertyValue(ExcelExtractProperties.HEADER_ROW_NB).isSet())) {
            ret.explanation(String.format("You must set exactly one of %s or %s.",
                    ExcelExtractProperties.FIELD_NAMES.getName(), ExcelExtractProperties.HEADER_ROW_NB.getName()))
                    .subject(getIdentifier())
                    .valid(false);
        }
        return Collections.singletonList(ret.build());
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ExcelExtractProperties.DESIRED_SHEETS);
        descriptors.add(ExcelExtractProperties.COLUMNS_TO_SKIP);
        descriptors.add(ExcelExtractProperties.FIELD_NAMES);
        descriptors.add(ExcelExtractProperties.ROWS_TO_SKIP);
        descriptors.add(ExcelExtractProperties.RECORD_TYPE);
        descriptors.add(ExcelExtractProperties.HEADER_ROW_NB);
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
                    String sheetName = "unknown";
                    List<String> headerNames = null;

                    try {
                        Sheet sheet = iter.next();
                        sheetName = sheet.getSheetName();
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
                            if (configuration.getHeaderRowNumber() != null &&
                                    configuration.getHeaderRowNumber().equals(row.getRowNum())) {
                                headerNames = extractFieldNamesFromRow(row);

                            }
                            if (count++ < configuration.getRowsToSkip()) {
                                continue;
                            }
                            Record current = handleRow(row, headerNames);
                            current.setField(Fields.rowNumber(row.getRowNum()))
                                    .setField(Fields.sheetName(sheetName));
                            ret.add(current);
                        }

                    } catch (Exception e) {
                        LOGGER.error("Unrecoverable exception occurred while processing excel sheet", e);
                        ret.add(new StandardRecord().addError(ProcessError.RECORD_CONVERSION_ERROR.getName(),
                                String.format("Unable to parse sheet %s: %s", sheetName, e.getMessage())));
                    }
                }
            }
        } catch (InvalidFormatException | NotOfficeXmlFileException ife) {
            LOGGER.error("Wrong or unsupported file format.", ife);
            ret.add(new StandardRecord().addError(ProcessError.INVALID_FILE_FORMAT_ERROR.getName(), ife.getMessage()));
        } catch (IOException ioe) {
            LOGGER.error("I/O Exception occurred while processing excel file", ioe);
            ret.add(new StandardRecord().addError(ProcessError.RUNTIME_ERROR.getName(), ioe.getMessage()));

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
    private Record handleRow(Row row, List<String> header) {
        Record ret = new StandardRecord().setTime(new Date());
        int index = 0;
        for (Cell cell : row) {
            if (configuration.getFieldNames() != null && index >= configuration.getFieldNames().size()) {
                //we've reached the end of mapping. Go to next row.
                break;
            }
            if (configuration.getColumnsToSkip().contains(cell.getColumnIndex())) {
                //skip this cell.
                continue;
            }
            String fieldName = header != null ? header.get(cell.getColumnIndex()) :
                    configuration.getFieldNames().get(index++);
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

    private List<String> extractFieldNamesFromRow(Row row) {
        return StreamUtils.asStream(row.cellIterator())
                .map(Cell::getStringCellValue)
                .map(s -> s.replaceAll("\\s+", "_"))
                .collect(Collectors.toList());
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
