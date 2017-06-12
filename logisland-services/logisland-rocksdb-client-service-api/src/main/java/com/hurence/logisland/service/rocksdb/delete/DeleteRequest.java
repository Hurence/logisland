package com.hurence.logisland.service.rocksdb.delete;

import org.rocksdb.WriteOptions;

public class DeleteRequest {

    private String family;
    private byte[] key;
    private WriteOptions wOptions;

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public WriteOptions getReadOption() {
        return wOptions;
    }

    public void setReadOption(WriteOptions rOptions) {
        this.wOptions = rOptions;
    }



}
