package com.hurence.webapiservice.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class BucketUtils {

    private static Logger LOGGER = LoggerFactory.getLogger(BucketUtils.class);

    private BucketUtils() {}
    /**
     *
     * @param totalPoint total number point to sample
     * @param maxPoint max number of point to return
     * @return the smaller bucket size so that the number of bucket is lesser or equals to maxPoint.
     * @note this is a naive implementation which is not optimal !
     */
    //TODO optimize this
    public static int calculBucketSize(int totalPoint, int maxPoint) {
        LOGGER.debug("total point {}", totalPoint);
        LOGGER.debug("max point {}", maxPoint);
        int bucketSize = BigDecimal.valueOf(totalPoint).divide(BigDecimal.valueOf(maxPoint), RoundingMode.CEILING).intValue();
        LOGGER.debug("first try {}", bucketSize);
        while (!testBucketSize(totalPoint, bucketSize)) {
            bucketSize++;
        }
        LOGGER.debug("found {}", bucketSize);
        int numberofPoint = totalPoint / bucketSize;
        LOGGER.debug("numberofPoint {}", numberofPoint);
        LOGGER.debug("----------------");
        return bucketSize;
    }

    private static boolean testBucketSize(int totalPoint, int bucketSize) {
        int remain = totalPoint % bucketSize;
        LOGGER.trace("--------");
        LOGGER.trace("trying bucketSize {}", bucketSize);
        LOGGER.trace("remain {}", remain);
        return remain == 0;
    }
}
