package com.hurence.webapiservice.historian.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MetricsSizeInfoImpl implements MetricsSizeInfo {

    private Map<String, MetricSizeInfo> metricsInfo = new HashMap<>();

    @Override
    public Set<String> getMetrics() {
        return metricsInfo.keySet();
    }

    @Override
    public MetricSizeInfo getMetricInfo(String metric) {
        return metricsInfo.get(metric);
    }

    @Override
    public long getTotalNumberOfPoints() {
        return metricsInfo.values().stream().mapToLong(metricInfo -> metricInfo.totalNumberOfPoints).sum();
    }

    @Override
    public long getTotalNumberOfChunks() {
        return metricsInfo.values().stream().mapToLong(metricInfo -> metricInfo.totalNumberOfChunks).sum();
    }

    @Override
    public boolean isEmpty() {
        return metricsInfo.isEmpty();
    }

    public void setMetricInfo(MetricSizeInfo metricInfo) {
        metricsInfo.put(metricInfo.metricName, metricInfo);
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder("MetricsSizeInfoImpl{");
        metricsInfo.values().forEach(strBuilder::append);
        strBuilder.append("}");
        return strBuilder.toString();
    }
}
