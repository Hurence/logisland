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

/**
 * @author Stan Salvador (stansalvador@hotmail.com)
 * @author f.lautenschlager
 */
class MemoryResidentMatrix implements CostMatrix {
    private static final double OUT_OF_WINDOW_VALUE = Double.POSITIVE_INFINITY;

    private final SearchWindow window;
    private double[] cellValues;
    private int[] colOffsets;


    public MemoryResidentMatrix(SearchWindow searchWindow) {
        window = searchWindow;
        cellValues = new double[window.size()];
        colOffsets = new int[window.maxI() + 1];

        // Fill in the offset matrix
        int currentOffset = 0;
        for (int i = window.minI(); i <= window.maxI(); i++) {
            colOffsets[i] = currentOffset;
            currentOffset += window.maxJforI(i) - window.minJforI(i) + 1;
        }
    }


    public void put(int col, int row, double value) {
        cellValues[colOffsets[col] + row - window.minJforI(col)] = value;
    }


    public double get(int col, int row) {
        if ((row < window.minJforI(col)) || (row > window.maxJforI(col)))
            return OUT_OF_WINDOW_VALUE;
        else
            return cellValues[colOffsets[col] + row - window.minJforI(col)];
    }


    public int size() {
        return cellValues.length;
    }

}
