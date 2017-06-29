package com.hurence.logisland.service.rocksdb.get;

import java.util.Arrays;

public class GetResponse {

    private String family;
    private byte[] key;
    private byte[] value;


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

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GetResponse)) return false;

        GetResponse that = (GetResponse) o;

        if (family != null ? !family.equals(that.family) : that.family != null) return false;
        if (!Arrays.equals(key, that.key)) return false;
        return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = family != null ? family.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
