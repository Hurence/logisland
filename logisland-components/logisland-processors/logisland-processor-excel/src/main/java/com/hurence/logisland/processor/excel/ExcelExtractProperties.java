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

import com.hurence.logisland.annotation.documentation.Category;
import com.hurence.logisland.annotation.documentation.ComponentCategory;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Category(ComponentCategory.PARSING)
public class ExcelExtractProperties implements Serializable {

    public static final PropertyDescriptor RECORD_TYPE = new PropertyDescriptor.Builder()
            .name("record.type")
            .description("Default type of record")
            .required(false)
            .defaultValue("excel_record")
            .build();

    public static final PropertyDescriptor DESIRED_SHEETS = new PropertyDescriptor
            .Builder().name("sheets")
            .displayName("Sheets to Extract")
            .description("Comma separated list of Excel document sheet names that should be extracted from the excel document. If this property" +
                    " is left blank then all of the sheets will be extracted from the Excel document. You can specify regular expressions." +
                    " Any sheets not specified in this value will be ignored.")
            .required(false)
            .defaultValue("")
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    /**
     * The number of rows to skip. Useful if you want to skip first row (usually the table header).
     */
    public static final PropertyDescriptor ROWS_TO_SKIP = new PropertyDescriptor
            .Builder().name("skip.rows")
            .displayName("Number of Rows to Skip")
            .description("The row number of the first row to start processing."
                    + "Use this to skip over rows of data at the top of your worksheet that are not part of the dataset."
                    + "Empty rows of data anywhere in the spreadsheet will always be skipped, no matter what this value is set to.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    /**
     * List of column numbers to skip. Empty means include anything.
     */
    public static final PropertyDescriptor COLUMNS_TO_SKIP = new PropertyDescriptor
            .Builder().name("skip.columns")
            .displayName("Columns To Skip")
            .description("Comma delimited list of column numbers to skip. Use the columns number and not the letter designation. "
                    + "Use this to skip over columns anywhere in your worksheet that you don't want extracted as part of the record.")
            .required(false)
            .defaultValue("")
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();


    /**
     * Mapping between column extracted and field names in a record.
     */
    public static final PropertyDescriptor FIELD_NAMES = new PropertyDescriptor
            .Builder().name("field.names")
            .displayName("Field names mapping")
            .description("The comma separated list representing the names of columns of extracted cells. Order matters!" +
                    " You should use either field.names either field.row.header but not both together.")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    /**
     * The row number to use to extract field name mapping.
     */
    public static final PropertyDescriptor HEADER_ROW_NB = new PropertyDescriptor
            .Builder().name("field.row.header")
            .displayName("Use a row header as field names mapping")
            .description("If set, field names mapping will be extracted from the specified row number." +
                    " You should use either field.names either field.row.header but not both together.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static class Configuration {

        /**
         * The list of patterns matching sheets to extract. Empty means everything.
         */
        private final List<Pattern> sheetsToExtract;
        /**
         * List of column numbers to skip. Empty means include anything.
         */
        private final List<Integer> columnsToSkip;
        /**
         * The number of rows to skip. Useful if you want to skip first row (usually the table header).
         */
        private final int rowsToSkip;

        /**
         * The prefix to use when defining fields' name in output records.
         */
        private final List<String> fieldNames;

        /**
         * The record type.
         */
        private final String recordType;

        /**
         * The row number to use to extract field name mapping.
         */
        private final Integer headerRowNumber;


        /**
         * Creates a configuration POJO from the {@link ProcessContext}
         *
         * @param context the current context.
         */
        public Configuration(ProcessContext context) {
            sheetsToExtract = Arrays.stream(context.getPropertyValue(DESIRED_SHEETS).asString().split(","))
                    .map(String::trim)
                    .filter(StringUtils::isNotBlank)
                    .map(Pattern::compile)
                    .collect(Collectors.toList());

            if (context.getPropertyValue(HEADER_ROW_NB).isSet()) {
                headerRowNumber = context.getPropertyValue(HEADER_ROW_NB).asInteger();
            } else {
                headerRowNumber = null;
            }

            columnsToSkip = Arrays.stream(context.getPropertyValue(COLUMNS_TO_SKIP).asString().split(","))
                    .filter(StringUtils::isNotBlank)
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
            rowsToSkip = context.getPropertyValue(ROWS_TO_SKIP).asInteger();

            if (context.getPropertyValue(FIELD_NAMES).isSet()) {
                fieldNames = Arrays.stream(context.getPropertyValue(FIELD_NAMES).asString().split(","))
                        .filter(StringUtils::isNotBlank)
                        .map(String::trim)
                        .collect(Collectors.toList());
            } else {
                fieldNames = null;
            }
            recordType = context.getPropertyValue(RECORD_TYPE).asString();
        }


        /**
         * The list of patterns matching sheet to extract. Empty means everything.
         *
         * @return a never null list.
         */
        public List<Pattern> getSheetsToExtract() {
            return sheetsToExtract;
        }


        /**
         * List of column numbers to skip. Empty means include anything.
         *
         * @return a never null {@link List}
         */
        public List<Integer> getColumnsToSkip() {
            return columnsToSkip;
        }

        /**
         * The number of rows to skip. Useful if you want to skip first row (usually the table header).
         *
         * @return
         */
        public int getRowsToSkip() {
            return rowsToSkip;
        }


        /**
         * Mapping between column extracted and field names in a record.
         *
         * @return
         */
        public List<String> getFieldNames() {
            return fieldNames;
        }

        /**
         * The record type.
         *
         * @return
         */
        public String getRecordType() {
            return recordType;
        }


        public Integer getHeaderRowNumber() {
            return headerRowNumber;
        }

        @Override
        public String toString() {
            return "Configuration{" +
                    "sheetsToExtract=" + sheetsToExtract +
                    ", columnsToSkip=" + columnsToSkip +
                    ", rowsToSkip=" + rowsToSkip +
                    ", fieldNames=" + fieldNames +
                    ", recordType='" + recordType + '\'' +
                    ", headerRowNumber=" + headerRowNumber +
                    '}';
        }
    }

}
