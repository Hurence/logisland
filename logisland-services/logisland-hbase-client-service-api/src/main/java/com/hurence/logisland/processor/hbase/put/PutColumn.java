package com.hurence.logisland.processor.hbase.put;

/**
 * Encapsulates the information for one column of a put operation.
 */
public class PutColumn {

    private final byte[] columnFamily;
    private final byte[] columnQualifier;
    private final byte[] buffer;


    public PutColumn(final byte[] columnFamily, final byte[] columnQualifier, final byte[] buffer) {
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.buffer = buffer;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    public byte[] getBuffer() {
        return buffer;
    }

}
