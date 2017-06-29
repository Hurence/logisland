package com.hurence.logisland.service.rocksdb.delete;

import org.rocksdb.WriteOptions;

import java.util.Arrays;

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

    public WriteOptions getWriteOptions() {
        return wOptions;
    }

    public void setWriteOptions(WriteOptions rOptions) {
        this.wOptions = rOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeleteRequest)) return false;

        DeleteRequest that = (DeleteRequest) o;

        if (family != null ? !family.equals(that.family) : that.family != null) return false;
        if (!Arrays.equals(key, that.key)) return false;
        return wOptions != null ? wOptions.equals(that.wOptions) : that.wOptions == null;
    }

    @Override
    public int hashCode() {
        int result = family != null ? family.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + (wOptions != null ? wOptions.hashCode() : 0);
        return result;
    }
}
