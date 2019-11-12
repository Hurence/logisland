package com.hurence.webapiservice.historian.impl;

public class MetricSizeInfo {

    public String metricName;
    public long totalNumberOfPoints;
    public long totalNumberOfChunks;

    @Override
    public String toString() {
        return "MetricSizeInfo{" +
                "metricName='" + metricName + '\'' +
                ", totalNumberOfPoints=" + totalNumberOfPoints +
                ", totalNumberOfChunks=" + totalNumberOfChunks +
                '}';
    }
}
