package com.hurence.webapiservice.timeseries;

import org.junit.jupiter.api.Test;

import static com.hurence.webapiservice.timeseries.BucketUtils.calculBucketSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BucketUtilsTest {

    @Test
    public void test40Point() {
        int totalNumberOfpoint = 40;
        assertEquals(2, calculBucketSize(totalNumberOfpoint, 39));
        assertEquals(20,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 39));
        assertEquals(2, calculBucketSize(totalNumberOfpoint, 35));
        assertEquals(20,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 35));
        assertEquals(2, calculBucketSize(totalNumberOfpoint, 30));
        assertEquals(20,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 30));
        assertEquals(2, calculBucketSize(totalNumberOfpoint, 25));
        assertEquals(20,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 25));
        assertEquals(4, calculBucketSize(totalNumberOfpoint, 15));
        assertEquals(10,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 10));
        assertEquals(4, calculBucketSize(totalNumberOfpoint, 10));
        assertEquals(10,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 10));
        assertEquals(5, calculBucketSize(totalNumberOfpoint, 9));
        assertEquals(8,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 9));
        assertEquals(5, calculBucketSize(totalNumberOfpoint, 8));
        assertEquals(8,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 8));
        assertEquals(8, calculBucketSize(totalNumberOfpoint, 7));
        assertEquals(5,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 7));
        assertEquals(8, calculBucketSize(totalNumberOfpoint, 6));
        assertEquals(5,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 6));
        assertEquals(8, calculBucketSize(totalNumberOfpoint, 5));
        assertEquals(5,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 5));
    }

    @Test
    public void test5000PointAndMax39() {
        int totalNumberOfpoint = 5000;
        assertEquals(4, calculBucketSize(totalNumberOfpoint, 2444));
        assertEquals(1250,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 2444));
        assertEquals(4, calculBucketSize(totalNumberOfpoint, 1333));
        assertEquals(1250,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 1333));
        assertEquals(8, calculBucketSize(totalNumberOfpoint, 888));
        assertEquals(625,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 888));
        assertEquals(10, calculBucketSize(totalNumberOfpoint, 500));
        assertEquals(500,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 500));
        assertEquals(25, calculBucketSize(totalNumberOfpoint, 243));
        assertEquals(200,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 243));
        assertEquals(50, calculBucketSize(totalNumberOfpoint, 122));
        assertEquals(100,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 122));
        assertEquals(100, calculBucketSize(totalNumberOfpoint, 86));
        assertEquals(50,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 86));
        assertEquals(200, calculBucketSize(totalNumberOfpoint, 34));
        assertEquals(25,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 34));
        assertEquals(500, calculBucketSize(totalNumberOfpoint, 12));
        assertEquals(10,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 12));
        assertEquals(1000, calculBucketSize(totalNumberOfpoint, 6));
        assertEquals(5,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 6));
        assertEquals(1000, calculBucketSize(totalNumberOfpoint, 5));
        assertEquals(5,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 5));
    }

    @Test
    public void test9999PointAndMax39() {
        int totalNumberOfpoint = 9999;
        assertEquals(99, calculBucketSize(totalNumberOfpoint, 300));
        assertEquals(101,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 300));
        assertEquals(9999, calculBucketSize(totalNumberOfpoint, 2));
        assertEquals(1,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 2));
        assertEquals(9999, calculBucketSize(totalNumberOfpoint, 1));
        assertEquals(1,totalNumberOfpoint / calculBucketSize(totalNumberOfpoint, 1));
    }
}
