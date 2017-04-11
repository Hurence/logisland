package com.hurence.logisland.hbase.scan;

import java.util.Arrays;

/**
 * Wrapper to encapsulate a column family and qualifier.
 */
public class Column {

    private final byte[] family;
    private final byte[] qualifier;

    public Column(byte[] family, byte[] qualifier) {
        this.family = family;
        this.qualifier = qualifier;
    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Column)) {
            return false;
        }

        final Column other = (Column) obj;
        return ((this.family == null && other.family == null)
                || (this.family != null && other.family != null && Arrays.equals(this.family, other.family)))
                && ((this.qualifier == null && other.qualifier == null)
                || (this.qualifier != null && other.qualifier != null && Arrays.equals(this.qualifier, other.qualifier)));
    }

    @Override
    public int hashCode() {
        int result = 37;
        if (family != null) {
            for (byte b : family) {
                result += (int)b;
            }
        }
        if (qualifier != null) {
            for (byte b : qualifier) {
                result += (int)b;
            }
        }
        return result;
    }

}
