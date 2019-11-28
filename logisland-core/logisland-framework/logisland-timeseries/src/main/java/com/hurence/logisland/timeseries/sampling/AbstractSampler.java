package com.hurence.logisland.timeseries.sampling;

public abstract class AbstractSampler<SAMPLED> implements Sampler<SAMPLED> {

    protected TimeSerieHandler<SAMPLED> timeSerieHandler;
    protected int bucketSize;

    public AbstractSampler(TimeSerieHandler<SAMPLED> timeSerieHandler, int bucketSize) {
        this.timeSerieHandler = timeSerieHandler;
        this.bucketSize = bucketSize;
    }
}
