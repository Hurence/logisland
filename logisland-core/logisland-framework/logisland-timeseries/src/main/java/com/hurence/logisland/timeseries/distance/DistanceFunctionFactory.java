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
package com.hurence.logisland.timeseries.distance;

/**
 * @author Stan Salvador (stansalvador@hotmail.com)
 * @author f.lautenschlager
 */
public final class DistanceFunctionFactory {

    private static DistanceFunction EUCLIDEAN_DIST_FN = new EuclideanDistance();
    private static DistanceFunction MANHATTAN_DIST_FN = new ManhattanDistance();
    private static DistanceFunction BINARY_DIST_FN = new BinaryDistance();

    private DistanceFunctionFactory() {
        //Avoid instances
    }

    /**
     * Method to get the implementation of the given distance function
     *
     * @param function the distance function
     * @return the implementation of the distance function
     */
    public static DistanceFunction getDistanceFunction(DistanceFunctionEnum function) {
        switch (function) {
            case EUCLIDEAN:
                return EUCLIDEAN_DIST_FN;
            case MANHATTAN:
                return MANHATTAN_DIST_FN;
            case BINARY:
                return BINARY_DIST_FN;
            default:
                throw new IllegalArgumentException("There is no DistanceFunction for the name " + function);
        }
    }
}