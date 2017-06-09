package com.hurence.logisland.service.rocksdb.put;

import org.rocksdb.WriteOptions;

/**
 * Encapsulates the information for one column of a put operation.
 */
public class ValuePutRequest {

    private byte[] family;
    private byte[] key;
    private byte[] value;
    private WriteOptions wOptions;

    public ValuePutRequest(){}

    public ValuePutRequest(final byte[] family, final byte[] key, final byte[] value, WriteOptions wOptions) {
        this.family = family;
        this.key = key;
        this.value = value;
        this.wOptions = wOptions;
    }


    public byte[] getFamily() {
        return family;
    }

    public void setFamily(byte[] family) {
        this.family = family;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public WriteOptions getwOptions() {
        return wOptions;
    }

    public void setwOptions(WriteOptions wOptions) {
        this.wOptions = wOptions;
    }
}
