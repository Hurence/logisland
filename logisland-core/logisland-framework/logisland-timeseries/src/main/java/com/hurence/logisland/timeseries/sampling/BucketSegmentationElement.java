package com.hurence.logisland.timeseries.sampling;

import java.util.Objects;

public class BucketSegmentationElement {
    public final long numbreOfBucket;
    public final int sizeOfBucket;

    public BucketSegmentationElement(long numbreOfBucket, int sizeOfBucket) {
        this.numbreOfBucket = numbreOfBucket;
        this.sizeOfBucket = sizeOfBucket;
    }

    @Override
    public String toString() {
        return "BucketSegmentationElement{" +
                "numbreOfBucket=" + numbreOfBucket +
                ", sizeOfBucket=" + sizeOfBucket +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BucketSegmentationElement that = (BucketSegmentationElement) o;
        return numbreOfBucket == that.numbreOfBucket &&
                sizeOfBucket == that.sizeOfBucket;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numbreOfBucket, sizeOfBucket);
    }
}
