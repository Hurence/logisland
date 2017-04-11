package com.hurence.logisland.hbase.put;


import com.hurence.logisland.record.Record;

import java.util.Collection;

/**
 * Wrapper to encapsulate all of the information for the Put along with the Record.
 */
public class PutRecord {

    private final String tableName;
    private final byte[] row;
    private final Collection<PutColumn> columns;
    private final Record flowFile;

    public PutRecord(String tableName, byte[] row, Collection<PutColumn> columns, Record flowFile) {
        this.tableName = tableName;
        this.row = row;
        this.columns = columns;
        this.flowFile = flowFile;
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
        return flowFile;
    }

    public boolean isValid() {
        if (tableName == null || tableName.trim().isEmpty() || null == row || flowFile == null || columns == null || columns.isEmpty()) {
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
