package com.hurence.webapiservice.modele;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;

public class SamplingAlgo {
    SamplingAlgorithm algo;
    long bucketSize;
    long maxPoint;

    public SamplingAlgo(SamplingAlgorithm algo, long bucketSize, long maxPoint) {
        this.algo = algo;
        this.bucketSize = bucketSize;
        this.maxPoint = maxPoint;
    }
}
