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
import com.hurence.logisland.timeseries.distance.DistanceFunction;
import com.hurence.logisland.timeseries.matrix.ColMajorCell;

import java.util.Iterator;

/**
 * @author Stan Salvador (stansalvador@hotmail.com)
 * @author f.lautenschlager
 */
public final class DTW {

    private DTW() {
        //avoid instances
    }

    public static double calcWarpCost(WarpPath path, MultivariateTimeSeries tsI, MultivariateTimeSeries tsJ, DistanceFunction distFn) {
        double totalCost = 0.0;

        for (int p = 0; p < path.size(); p++) {
            final ColMajorCell currWarp = path.get(p);
            totalCost += distFn.calcDistance(tsI.getMeasurementVector(currWarp.getCol()), tsJ.getMeasurementVector(currWarp.getRow()));
        }
        return totalCost;
    }

    public static TimeWarpInfo getWarpInfoBetween(MultivariateTimeSeries tsI, MultivariateTimeSeries tsJ, DistanceFunction distFn) {
        return dynamicTimeWarp(tsI, tsJ, distFn);
    }


    private static TimeWarpInfo dynamicTimeWarp(MultivariateTimeSeries tsI, MultivariateTimeSeries tsJ, DistanceFunction distFn) {
        //     COST MATRIX:
        //   5|_|_|_|_|_|_|E| E = min Global Cost
        //   4|_|_|_|_|_|_|_| S = Start point
        //   3|_|_|_|_|_|_|_| each cell = min global cost to get to that point
        // j 2|_|_|_|_|_|_|_|
        //   1|_|_|_|_|_|_|_|
        //   0|S|_|_|_|_|_|_|
        //     0 1 2 3 4 5 6
        //            i
        //   access is M(i,j)... column-row

        final double[][] costMatrix = new double[tsI.size()][tsJ.size()];
        final int maxI = tsI.size() - 1;
        final int maxJ = tsJ.size() - 1;

        // Calculate the values for the first column, from the bottom up.
        costMatrix[0][0] = distFn.calcDistance(tsI.getMeasurementVector(0), tsJ.getMeasurementVector(0));
        for (int j = 1; j <= maxJ; j++)
            costMatrix[0][j] = costMatrix[0][j - 1] + distFn.calcDistance(tsI.getMeasurementVector(0), tsJ.getMeasurementVector(j));

        for (int i = 1; i <= maxI; i++)   // i = columns
        {
            // Calculate the value for the bottom row of the current column
            //    (i,0) = LocalCost(i,0) + GlobalCost(i-1,0)
            costMatrix[i][0] = costMatrix[i - 1][0] + distFn.calcDistance(tsI.getMeasurementVector(i), tsJ.getMeasurementVector(0));

            for (int j = 1; j <= maxJ; j++)  // j = rows
            {
                // (i,j) = LocalCost(i,j) + minGlobalCost{(i-1,j),(i-1,j-1),(i,j-1)}
                final double minGlobalCost = Math.min(costMatrix[i - 1][j],
                        Math.min(costMatrix[i - 1][j - 1],
                                costMatrix[i][j - 1]));
                costMatrix[i][j] = minGlobalCost + distFn.calcDistance(tsI.getMeasurementVector(i), tsJ.getMeasurementVector(j));
            }
        }


        // Minimum Cost is at (maxIi,maxJ)
        final double minimumCost = costMatrix[maxI][maxJ];


        // Find the Warp Path by searching the matrix from the solution at
        //    (maxI, maxJ) to the beginning at (0,0).  At each step move through
        //    the matrix 1 step left, down, or diagonal, whichever has the
        //    smallest cost.  Favoer diagonal moves and moves towards the i==j
        //    axis to break ties.
        final WarpPath minCostPath = new WarpPath(maxI + maxJ - 1);
        int i = maxI;
        int j = maxJ;
        minCostPath.add(i, j);
        while ((i > 0) || (j > 0)) {
            // Find the costs of moving in all three possible directions (left,
            //    down, and diagonal (down and left at the same time).
            final double diagCost;
            final double leftCost;
            final double downCost;

            if ((i > 0) && (j > 0))
                diagCost = costMatrix[i - 1][j - 1];
            else
                diagCost = Double.POSITIVE_INFINITY;

            if (i > 0)
                leftCost = costMatrix[i - 1][j];
            else
                leftCost = Double.POSITIVE_INFINITY;

            if (j > 0)
                downCost = costMatrix[i][j - 1];
            else
                downCost = Double.POSITIVE_INFINITY;

            // Determine which direction to move in.  Prefer moving diagonally and
            //    moving towards the i==j axis of the matrix if there are ties.
            if ((diagCost <= leftCost) && (diagCost <= downCost)) {
                i--;
                j--;
            } else if ((leftCost < diagCost) && (leftCost < downCost))
                i--;
            else if ((downCost < diagCost) && (downCost < leftCost))
                j--;
            else if (i <= j)  // leftCost==rightCost > diagCost
                j--;
            else   // leftCost==rightCost > diagCost
                i--;

            // Add the current step to the warp path.
            minCostPath.add(i, j);
        }

        return new TimeWarpInfo(minimumCost, minCostPath, tsI.size(), tsJ.size());
    }


    private static void calcCostMatrix(MultivariateTimeSeries tsI, MultivariateTimeSeries tsJ, DistanceFunction distFn, CostMatrix costMatrix, Iterator<ColMajorCell> matrixIterator) {
        while (matrixIterator.hasNext()) {
            final ColMajorCell currentCell = matrixIterator.next();  // current cell being filled
            final int i = currentCell.getCol();
            final int j = currentCell.getRow();

            if ((i == 0) && (j == 0))      // bottom left cell (first row AND first column)
                costMatrix.put(i, j, distFn.calcDistance(tsI.getMeasurementVector(0), tsJ.getMeasurementVector(0)));
            else if (i == 0)           // first column
            {
                costMatrix.put(i, j, distFn.calcDistance(tsI.getMeasurementVector(0), tsJ.getMeasurementVector(j)) + costMatrix.get(i, j - 1));
            } else if (j == 0)             // first row
            {
                costMatrix.put(i, j, distFn.calcDistance(tsI.getMeasurementVector(i), tsJ.getMeasurementVector(0)) + costMatrix.get(i - 1, j));
            } else                         // not first column or first row
            {
                final double minGlobalCost = Math.min(costMatrix.get(i - 1, j), Math.min(costMatrix.get(i - 1, j - 1), costMatrix.get(i, j - 1)));
                costMatrix.put(i, j, minGlobalCost + distFn.calcDistance(tsI.getMeasurementVector(i), tsJ.getMeasurementVector(j)));
            }
        }
    }

    public static TimeWarpInfo getWarpInfoBetween(MultivariateTimeSeries tsI, MultivariateTimeSeries tsJ, SearchWindow window, DistanceFunction distFn) {
        return constrainedTimeWarp(tsI, tsJ, window, distFn);
    }


    private static TimeWarpInfo constrainedTimeWarp(MultivariateTimeSeries tsI, MultivariateTimeSeries tsJ, SearchWindow window, DistanceFunction distFn) {
        //     COST MATRIX:
        //   5|_|_|_|_|_|_|E| E = min Global Cost
        //   4|_|_|_|_|_|_|_| S = Start point
        //   3|_|_|_|_|_|_|_| each cell = min global cost to get to that point
        // j 2|_|_|_|_|_|_|_|
        //   1|_|_|_|_|_|_|_|
        //   0|S|_|_|_|_|_|_|
        //     0 1 2 3 4 5 6
        //            i
        //   access is M(i,j)... column-row
        final WindowMatrix costMatrix = new WindowMatrix(window);
        final int maxI = tsI.size() - 1;
        final int maxJ = tsJ.size() - 1;

        // Get an iterator that traverses the window cells in the order that the cost matrix is filled.
        //    (first to last row (1..maxI), bottom to top (1..MaxJ)
        final Iterator<ColMajorCell> matrixIterator = window.iterator();

        calcCostMatrix(tsI, tsJ, distFn, costMatrix, matrixIterator);

        // Minimum Cost is at (maxI, maxJ)
        final double minimumCost = costMatrix.get(maxI, maxJ);

        // Find the Warp Path by searching the matrix from the solution at
        //    (maxI, maxJ) to the beginning at (0,0).  At each step move through
        //    the matrix 1 step left, down, or diagonal, whichever has the
        //    smallest cost.  Favoer diagonal moves and moves towards the i==j
        //    axis to break ties.
        final WarpPath minCostPath = new WarpPath(maxI + maxJ - 1);
        int i = maxI;
        int j = maxJ;
        minCostPath.add(i, j);
        while ((i > 0) || (j > 0)) {
            // Find the costs of moving in all three possible directions (left,
            //    down, and diagonal (down and left at the same time).
            final double diagCost;
            final double leftCost;
            final double downCost;

            if ((i > 0) && (j > 0))
                diagCost = costMatrix.get(i - 1, j - 1);
            else
                diagCost = Double.POSITIVE_INFINITY;

            if (i > 0)
                leftCost = costMatrix.get(i - 1, j);
            else
                leftCost = Double.POSITIVE_INFINITY;

            if (j > 0)
                downCost = costMatrix.get(i, j - 1);
            else
                downCost = Double.POSITIVE_INFINITY;

            // Determine which direction to move in.  Prefer moving diagonally and
            //    moving towards the i==j axis of the matrix if there are ties.
            if ((diagCost <= leftCost) && (diagCost <= downCost)) {
                i--;
                j--;
            } else if ((leftCost < diagCost) && (leftCost < downCost))
                i--;
            else if ((downCost < diagCost) && (downCost < leftCost))
                j--;
            else if (i <= j)  // leftCost==rightCost > diagCost
                j--;
            else   // leftCost==rightCost > diagCost
                i--;

            // Add the current step to the warp path.
            minCostPath.add(i, j);
        }  // end while loop

        return new TimeWarpInfo(minimumCost, minCostPath, tsI.size(), tsJ.size());
    }
}
