package com.hurence.webapiservice.modele;

public class SamplingAlgo {
    SAMPLING algo;
    long bucketSize;
    long maxPoint;

    public SamplingAlgo(SAMPLING algo, long bucketSize, long maxPoint) {
        this.algo = algo;
        this.bucketSize = bucketSize;
        this.maxPoint = maxPoint;
    }
}
