package com.caseystella.analytics;

import java.util.Map;

public class DataPoint {
    private long timestamp;
    private double value;
    private Map<String, String> metadata;
    private String source;

    public DataPoint() {

    }

    public DataPoint(long timestamp, double value, Map<String, String> metadata, String source) {
        this.timestamp = timestamp;
        this.value = value;
        this.metadata = metadata;
        this.source = source;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }



    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataPoint dataPoint = (DataPoint) o;

        if (getTimestamp() != dataPoint.getTimestamp()) return false;
        if (Double.compare(dataPoint.getValue(), getValue()) != 0) return false;
        if (getMetadata() != null ? !getMetadata().equals(dataPoint.getMetadata()) : dataPoint.getMetadata() != null)
            return false;
        return source != null ? source.equals(dataPoint.source) : dataPoint.source == null;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (int) (getTimestamp() ^ (getTimestamp() >>> 32));
        temp = Double.doubleToLongBits(getValue());
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (getMetadata() != null ? getMetadata().hashCode() : 0);
        result = 31 * result + (source != null ? source.hashCode() : 0);
        return result;
    }
    @Override
    public String toString() {
        return "(" +
                "timestamp=" + timestamp +
                ", value=" + value +
                ", metadata=" + metadata +
                ", source=" + source+
                ')';
    }

}
