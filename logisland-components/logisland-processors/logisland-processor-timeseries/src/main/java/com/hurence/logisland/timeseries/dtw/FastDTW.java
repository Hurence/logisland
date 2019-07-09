/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Stan Salvador (stansalvador@hotmail.com), Philip Chan (pkc@cs.fit.edu), QAware GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.hurence.logisland.timeseries.dtw;


import com.hurence.logisland.timeseries.MultivariateTimeSeries;
import com.hurence.logisland.timeseries.PAA;
import com.hurence.logisland.timeseries.distance.DistanceFunction;

/**
 * @author Stan Salvador (stansalvador@hotmail.com)
 * @author f.lautenschlager
 */
public final class FastDTW {

    private FastDTW() {
        //avoid instances
    }

    public static TimeWarpInfo getWarpInfoBetween(final MultivariateTimeSeries tsI, final MultivariateTimeSeries tsJ, int searchRadius, final DistanceFunction distFn) {
        if (tsI.size() == 0 || tsJ.size() == 0) {
            return new TimeWarpInfo(0, new WarpPath(0), tsI.size(), tsJ.size());
        }
        return fastDTW(tsI, tsJ, searchRadius, distFn);
    }


    private static TimeWarpInfo fastDTW(final MultivariateTimeSeries tsI, final MultivariateTimeSeries tsJ, int searchRadius, final DistanceFunction distFn) {
        if (searchRadius < 0)
            searchRadius = 0;

        final int minTSsize = searchRadius + 2;

        if ((tsI.size() <= minTSsize) || (tsJ.size() <= minTSsize)) {
            // Perform full Dynamic Time Warping.
            return DTW.getWarpInfoBetween(tsI, tsJ, distFn);
        } else {
            final PAA shrunkI = new PAA(tsI, tsI.size() / 2);
            final PAA shrunkJ = new PAA(tsJ, tsJ.size() / 2);

            // Determine the search window that constrains the area of the cost matrix that will be evaluated based on
            //    the warp path found at the previous resolution (smaller time series).
            final WarpPath warpPath = getWarpInfoBetween(shrunkI, shrunkJ, searchRadius, distFn).getPath();
            final SearchWindow window = new ExpandedResWindow(tsI, tsJ, shrunkI, shrunkJ, warpPath, searchRadius);

            // Find the optimal warp path through this search window constraint.
            return DTW.getWarpInfoBetween(tsI, tsJ, window, distFn);
        }
    }
}
