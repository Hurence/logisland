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

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;

/**
 * Incapsulated field used by record of this processor.
 */
public class Fields {

    public static final String SHEET_NAME = "excel_extract_sheet_name";
    public static final String SOURCE_FILE_NAME = "source_file_name";
    public static final String ROW_NUMBER = "excel_extract_row_number";
    public static final String RECORD_TYPE = FieldDictionary.RECORD_TYPE;

    /**
     * Creates a field for the sheet name.
     *
     * @param name
     * @return
     */
    public static Field sheetName(String name) {
        return new Field(SHEET_NAME, FieldType.STRING, name);
    }


    /**
     * Creates a field for the extract record row number.
     *
     * @param number
     * @return
     */
    public static Field rowNumber(long number) {
        return new Field(ROW_NUMBER, FieldType.LONG, number);
    }

    /**
     * Creates a field for the record type.
     *
     * @param recordType
     * @return
     */
    public static Field recordType(String recordType) {
        return new Field(RECORD_TYPE, FieldType.STRING, recordType);
    }


}
