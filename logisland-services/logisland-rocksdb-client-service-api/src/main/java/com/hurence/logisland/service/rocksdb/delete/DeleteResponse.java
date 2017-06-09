package com.hurence.logisland.service.rocksdb.delete;

public class DeleteResponse {

    private byte[] family;
    private byte[] key;
    private byte[] value;


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
}
