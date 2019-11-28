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
     * @param totalNumberOfPoint total number point to sample
     * @param maxPoint max number of point to return
     * @return the smaller bucket size so that the number of bucket is lesser or equals to maxPoint.
     * @note this is a naive implementation which is not optimal !
     */

    public static int calculBucketSize(int totalNumberOfPoint, int maxPoint) {
        long totalNumberOfPointLong = totalNumberOfPoint;
        return calculBucketSize(totalNumberOfPointLong, maxPoint);
    }

    public static int calculBucketSize(long totalNumberOfPoint, int maxPoint) {
        BigDecimal totalNumberOfPointBigDec = BigDecimal.valueOf(totalNumberOfPoint);
        BigDecimal maxPointBigDec = BigDecimal.valueOf(maxPoint);
        BigDecimal mainBucketSize = totalNumberOfPointBigDec.divide(maxPointBigDec, RoundingMode.CEILING);
        BigDecimal remainderBigDec = totalNumberOfPointBigDec.remainder(mainBucketSize);
        LOGGER.debug("total point {}, max point {} :\n found bucket size of {} with a remain of {}",
                totalNumberOfPoint, maxPoint, mainBucketSize, remainderBigDec);
        return mainBucketSize.intValue();
    }
}
