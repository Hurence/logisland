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
package com.hurence.logisland.service.hbase.put;


import com.hurence.logisland.record.Record;

import java.util.Collection;

/**
 * Wrapper to encapsulate all of the information for the Put along with the Record.
 */
public class PutRecord {

    private final String tableName;
    private final byte[] row;
    private final Collection<PutColumn> columns;
    private final Record record;

    public PutRecord(String tableName, byte[] row, Collection<PutColumn> columns, Record record) {
        this.tableName = tableName;
        this.row = row;
        this.columns = columns;
        this.record = record;
    }

    public String getTableName() {
        return tableName;
    }

    public byte[] getRow() {
        return row;
    }

    public Collection<PutColumn> getColumns() {
        return columns;
    }

    public Record getRecord() {
        return record;
    }

    public boolean isValid() {
        if (tableName == null || tableName.trim().isEmpty() || null == row || record == null || columns == null || columns.isEmpty()) {
            return false;
        }

        for (PutColumn column : columns) {
            if (null == column.getColumnQualifier() || null == column.getColumnFamily() || column.getBuffer() == null) {
                return false;
            }
        }

        return true;
    }

}
