/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
