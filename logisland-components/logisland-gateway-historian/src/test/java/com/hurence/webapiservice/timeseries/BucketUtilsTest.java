package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.timeseries.sampling.BucketSegmentationElement;
import com.hurence.logisland.timeseries.sampling.BucketingStrategy;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.hurence.webapiservice.timeseries.BucketUtils.calculBucketSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BucketUtilsTest {

    @Test
    public void test40Point() {
        int totalNumberOfpoint = 40;
        testCalculBucket(totalNumberOfpoint, 39, 2, 20);
        testCalculBucket(totalNumberOfpoint, 35, 2, 20);
        testCalculBucket(totalNumberOfpoint, 30, 2, 20);
        testCalculBucket(totalNumberOfpoint, 25, 2, 20);
        testCalculBucket(totalNumberOfpoint, 15, 3, 13);
        testCalculBucket(totalNumberOfpoint, 10, 4, 10);
        testCalculBucket(totalNumberOfpoint, 9, 5, 8);
        testCalculBucket(totalNumberOfpoint, 8, 5, 8);
        testCalculBucket(totalNumberOfpoint, 7, 6, 6);
        testCalculBucket(totalNumberOfpoint, 6, 7, 5);
        testCalculBucket(totalNumberOfpoint, 5, 8, 5);
    }

    @Test
    public void test5000PointAndMax39() {
        int totalNumberOfpoint = 5000;
        testCalculBucket(totalNumberOfpoint, 2444, 3, 1666);
        testCalculBucket(totalNumberOfpoint, 1333, 4, 1250);
        testCalculBucket(totalNumberOfpoint, 888, 6, 833);
        testCalculBucket(totalNumberOfpoint, 500, 10, 500);
        testCalculBucket(totalNumberOfpoint, 243, 21, 238);
        testCalculBucket(totalNumberOfpoint, 122, 41, 121);
        testCalculBucket(totalNumberOfpoint, 86, 59, 84);
        testCalculBucket(totalNumberOfpoint, 34, 148, 33);
        testCalculBucket(totalNumberOfpoint, 12, 417, 11);
        testCalculBucket(totalNumberOfpoint, 6, 834, 5);
        testCalculBucket(totalNumberOfpoint, 5, 1000, 5);
    }

    @Test
    public void test9999PointAndMax39() {
        int totalNumberOfpoint = 9999;
        testCalculBucket(totalNumberOfpoint, 300, 34, 294);
        testCalculBucket(totalNumberOfpoint, 2, 5000, 1);
        testCalculBucket(totalNumberOfpoint, 1, 9999, 1);
        testCalculBucket(totalNumberOfpoint, 850, 12, 833);
    }

    @Test
    public void testSpecificBucketingThatCausedProblemInThePast() {
        testCalculBucket(30467, 850, 36, 846);
        testCalculBucket(75458, 850, 89, 847);
        testCalculBucket(6553, 850, 8, 819);
        testCalculBucket(12296, 850, 15, 819);
        testCalculBucket(18441, 850, 22, 838);
    }

    public void testCalculBucket(int totalNumberOfpoint, int maxPointAsked,
                                 int bucketSizeExpected, int numberOfPointExpectedOnceSampled) {
        assertEquals(bucketSizeExpected, calculBucketSize(totalNumberOfpoint, maxPointAsked));
        assertEquals(numberOfPointExpectedOnceSampled,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, maxPointAsked));
    }
}
