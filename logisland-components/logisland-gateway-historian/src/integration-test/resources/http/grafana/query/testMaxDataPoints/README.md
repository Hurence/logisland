# Description

Those test aim to verify that the number of datapoints returned is not higher that the amount requested.
The difficulty is to define the correct size of the buckets. There is many ways to solve this problem.
At the moment I thought of two solutions that I detail in conclusion section.

The test use the same defined points in historian, the only difference is the **maxDataPoints** parameter in the request.
There is 40 points injected in historian, then every test ask to have at max X points.

For example here some scenario I thought about :
maxDataPoints: 30 -> 10 points from buckets of size 2, 20 points from buckets of size 1
                 or 20 points from buckets of size 2
                 or 30 points from buckets of size 1 (eliminate other points...)
maxDataPoints: 10 -> 10 points from buckets of size 4
maxDataPoints: 9 -> 8 points from buckets of size 5
                 or 8 points from buckets of size 4 and 1 point from a bucket of size 8
                 or 4 points from buckets of size 5 and 5 point from a bucket of size 6
maxDataPoints: 8 -> 8 points from buckets of size 5
maxDataPoints: 7 -> 6 points from buckets of size 6
                 or ...
maxDataPoints: 6 -> 6 points from buckets of size 6
                 or ...
maxDataPoints: 5 -> 5 points from buckets of size 8

There is several choice to take here :
- do we choose to return exactly the number of datapoints requested ? Even if it imply buckets of different size ?
    - if yes then do we choose to have one extra size bucket or to spread over all buckets (max diff between buckets would be a few points)
- or do we choose to have homogeneus buckets ? That means that we could return several points less than asked as maximum

This is a complex question that should certainly depend on context. In my opinion the maxDataPoint purpose is to not retrieve too many points at the same time,
so we do not have to be very accurate. If a user wants exactly a number of points, another way of doing it should choose.

# Current Choice

Here the current behaviour defined in the tests (which is improvable in my opinion) :

maxDataPoints: 39 -> 20 points from buckets of size 2
maxDataPoints: 35 -> 20 points from buckets of size 2
maxDataPoints: 30 -> 20 points from buckets of size 2
maxDataPoints: 25 -> 20 points from buckets of size 2
maxDataPoints: 15 -> 10 points from buckets of size 4
maxDataPoints: 10 -> 10 points from buckets of size 4
maxDataPoints: 9 -> 8 points from buckets of size 5
maxDataPoints: 8 -> 8 points from buckets of size 5
maxDataPoints: 7 -> 5 points from buckets of size 8
maxDataPoints: 6 -> 5 points from buckets of size 8
maxDataPoints: 5 -> 5 points from buckets of size 8

Another naive solution would be :

maxDataPoints: 39 -> 38 points from buckets of size 1, 1 point from bucket of size 2
maxDataPoints: 35 -> 34 points from buckets of size 1, 1 point from bucket of size 6  TODO in test
maxDataPoints: 30 -> 29 points from buckets of size 1, 1 point from bucket of size 11 TODO in test
maxDataPoints: 25 -> 24 points from buckets of size 1, 1 point from bucket of size 26 TODO in test
maxDataPoints: 15 -> 14 points from buckets of size 2, 1 point from bucket of size 12
maxDataPoints: 10 -> 10 points from buckets of size 4
maxDataPoints: 9 -> 8 points from buckets of size 5
maxDataPoints: 8 -> 8 points from buckets of size 5
maxDataPoints: 7 -> 5 points from buckets of size 8
maxDataPoints: 6 -> 5 points from buckets of size 8
maxDataPoints: 5 -> 5 points from buckets of size 8

# Best solution in my opinion

I think the better choice is to uniform buckets if possible and if not defined buckets of different size but with few difference in size.
Unless maybe the user ask for a specific bucket size. This is not yet implemented for simplicity.
This would give something like that :

maxDataPoints: 39 -> 1 points from buckets of size 2, 38 points from buckets of size 1
maxDataPoints: 35 -> 5 points from buckets of size 2, 30 points from buckets of size 1 
maxDataPoints: 30 -> 10 points from buckets of size 2, 20 points from buckets of size 1
maxDataPoints: 25 -> 15 points from buckets of size 2, 10 points from buckets of size 1
maxDataPoints: 15 -> 10 points from buckets of size 3, 5 points from buckets of size 2
maxDataPoints: 10 -> 10 points from buckets of size 4
maxDataPoints: 9 -> 8 points from buckets of size 4
maxDataPoints: 8 -> 8 points from buckets of size 5
maxDataPoints: 7 -> 5 points from buckets of size 8
maxDataPoints: 6 -> 5 points from buckets of size 8
maxDataPoints: 5 -> 5 points from buckets of size 8

# Evolution

Please make sure to update this documentation if the behavior should be changed. And make sure to 
be able to use different strategy so that we can always old defined strategy if we want to.  