package com.hurence.logisland.service.rocksdb.get;

import org.rocksdb.ReadOptions;

public class GetRequest {

    private byte[] family;
    private byte[] key;
    private ReadOptions rOptions;

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

    public ReadOptions getReadOption() {
        return rOptions;
    }

    public void setReadOption(ReadOptions rOptions) {
        this.rOptions = rOptions;
    }



}
