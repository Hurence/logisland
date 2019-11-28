package com.hurence.webapiservice.modele;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;

public class SamplingConf {
    private SamplingAlgorithm algo;
    private int bucketSize;
    private int maxPoint;

    public SamplingConf(SamplingAlgorithm algo, int bucketSize, int maxPoint) {
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

    public int getMaxPoint() {
        return maxPoint;
    }

    @Override
    public String toString() {
        return "SamplingConf{" +
                "algo=" + algo +
                ", bucketSize=" + bucketSize +
                ", maxPoint=" + maxPoint +
                '}';
    }
}
