package com.hurence.webapiservice.historian.impl;

import java.util.Set;

public interface MetricsSizeInfo {

    public Set<String> getMetrics();

    public MetricSizeInfo getMetricInfo(String metric);

    public long getTotalNumberOfPoints();

    public long getTotalNumberOfChunks();

    boolean isEmpty();
}

