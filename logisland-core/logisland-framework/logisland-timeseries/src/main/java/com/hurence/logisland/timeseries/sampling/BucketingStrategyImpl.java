package com.hurence.logisland.timeseries.sampling;

import java.util.List;

public class BucketingStrategyImpl implements BucketingStrategy {
    private List<BucketSegmentationElement> bucketSegmentationList;

    public BucketingStrategyImpl(List<BucketSegmentationElement> bucketSegmentationList) {
        this.bucketSegmentationList = bucketSegmentationList;
    }

    @Override
    public long getTotalNumberOfBucket() {
        return bucketSegmentationList.stream().mapToLong(b -> b.numbreOfBucket).sum();
    }

    @Override
    public long getTotalNumberOfPointSampled() {
        return bucketSegmentationList.stream().mapToLong(b -> b.sizeOfBucket * b.numbreOfBucket).sum();
    }

    @Override
    public int getStartPointOfBucket(long bucketNumber) {
        throw new IllegalStateException("Not implemented yet");//TODO
    }

    @Override
    public int getEndPointOfBucket(long bucketNumber) {
        throw new IllegalStateException("Not implemented yet");//TODO
    }

    @Override
    public String toString() {
        return "BucketingStrategyImpl{" +
                "bucketSegmentationList=" + bucketSegmentationList +
                '}';
    }
}
