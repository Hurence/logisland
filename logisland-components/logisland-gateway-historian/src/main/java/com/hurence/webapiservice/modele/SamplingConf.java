package com.hurence.webapiservice.modele;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;

public class SamplingConf {
    private SamplingAlgorithm algo;
    private int bucketSize;
    private long maxPoint;

    public SamplingConf(SamplingAlgorithm algo, int bucketSize, long maxPoint) {
        this.algo = algo;
        this.bucketSize = bucketSize;
        this.maxPoint = maxPoint;
    }

    public SamplingAlgorithm getAlgo() {
        return algo;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public long getMaxPoint() {
        return maxPoint;
    }
}
