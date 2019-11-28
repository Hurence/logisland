package com.hurence.logisland.timeseries.sampling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

//TODO this is an attempt to be able to use more complex bucketing in sampler. Aborted this because I found a simpler and
// faster way to solve my problem.
public interface BucketingStrategy {

    static Logger LOGGER = LoggerFactory.getLogger(BucketingStrategy.class);

    static BucketingStrategy calculBucketStrategy(long totalNumberOfPoint, int maxPoint) {
        List<BucketSegmentationElement> buckets = new ArrayList<>();
        BigDecimal totalNumberOfPointBigDec = BigDecimal.valueOf(totalNumberOfPoint);
        BigDecimal maxPointBigDec = BigDecimal.valueOf(maxPoint);
        BigDecimal mainBucketSize = totalNumberOfPointBigDec.divide(maxPointBigDec, RoundingMode.CEILING);
        BigDecimal numberOfMainBucket = totalNumberOfPointBigDec.divide(mainBucketSize, RoundingMode.FLOOR);
        buckets.add(new BucketSegmentationElement(numberOfMainBucket.longValue() , mainBucketSize.intValue()));
        BigDecimal remainderBigDec = totalNumberOfPointBigDec.remainder(mainBucketSize);
        if (remainderBigDec.intValue() != 0) {
            buckets.add(new BucketSegmentationElement(1 , remainderBigDec.intValue()));
        }
        BucketingStrategy bucketingStrategy = new BucketingStrategyImpl(buckets);
        LOGGER.debug("total point {}, max point {} :\n found {}", totalNumberOfPoint, maxPoint, bucketingStrategy);
        return bucketingStrategy;
    }

    long getTotalNumberOfBucket();

    long getTotalNumberOfPointSampled();

    int getStartPointOfBucket(long bucketNumber);

    int getEndPointOfBucket(long bucketNumber);
}
