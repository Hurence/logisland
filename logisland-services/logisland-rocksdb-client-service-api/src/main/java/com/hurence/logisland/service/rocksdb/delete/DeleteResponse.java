package com.hurence.logisland.service.rocksdb.delete;

import java.util.Arrays;

public class DeleteResponse {

    private String family;
    private byte[] key;


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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeleteResponse)) return false;

        DeleteResponse that = (DeleteResponse) o;

        if (family != null ? !family.equals(that.family) : that.family != null) return false;
        return Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        int result = family != null ? family.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }
}
