package com.hurence.webapiservice.timeseries;

import com.hurence.webapiservice.modele.SamplingConf;

public class MultiTimeSeriesExtractorUsingPreAgg extends MultiTimeSeriesExtracterImpl {

    public MultiTimeSeriesExtractorUsingPreAgg(long from, long to, SamplingConf samplingConf) {
        super(from, to, samplingConf);
    }

    @Override
    protected TimeSeriesExtracter createTimeSeriesExtractor(String metricName) {
        return new TimeSeriesExtracterUsingPreAgg(metricName, from, to, samplingConf, totalNumberOfPointByMetrics.get(metricName));
    }
}
