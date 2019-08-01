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



import com.hurence.logisland.timeseries.matrix.ColMajorCell;

import java.util.*;

/**
 * @author Stan Salvador (stansalvador@hotmail.com)
 * @author f.lautenschlager
 */
abstract public class SearchWindow {
    private final int[] minValues;
    private final int[] maxValues;
    private final int maxJ;
    private int size;
    private int modCount;


    public SearchWindow(int tsIsize, int tsJsize) {
        minValues = new int[tsIsize];
        maxValues = new int[tsIsize];
        Arrays.fill(minValues, -1);
        maxJ = tsJsize - 1;
        size = 0;
        modCount = 0;
    }


    public final boolean isInWindow(int i, int j) {
        return (i >= minI()) && (i <= maxI()) && (minValues[i] <= j) && (maxValues[i] >= j);
    }


    public final int minI() {
        return 0;
    }


    public final int maxI() {
        return minValues.length - 1;
    }


    public final int minJ() {
        return 0;
    }


    public final int maxJ() {
        return maxJ;
    }


    public final int minJforI(int i) {
        return minValues[i];
    }


    public final int maxJforI(int i) {
        return maxValues[i];
    }


    public final int size() {
        return size;
    }


    // Iterates through all cells in the search window in the order that Dynamic
    //    Time Warping needs to evaluate them. (first to last column (0..maxI),
    //    bottom up  (o..maxJ))
    public final Iterator<ColMajorCell> iterator() {
        return new SearchWindowIterator(this);
    }


    public final String toString() {
        final StringBuilder outStr = new StringBuilder();

        for (int i = minI(); i <= maxI(); i++) {
            outStr.append("i=").append(i).append(", j=").append(minValues[i]).append("...").append(maxValues[i]);
            if (i != maxI())
                outStr.append("\n");
        }

        return outStr.toString();
    }


    protected int getModCount() {
        return modCount;
    }


    //    Expands the current window by a s pecified radius.
    protected final void expandWindow(int radius) {
        if (radius > 0) {
            // Expand the search window by one before expanding by the remainder of the radius because the function
            //    "expandSearchWindow(.) may not work correctly if the path has a width of only 1.
            expandSearchWindow(1);
            expandSearchWindow(radius - 1);
        }
    }


    private void expandSearchWindow(int radius) {
        if (radius > 0)  // if radius <=0 then no search is necessary, use the current search window
        {
            // Add all cells in the current Window to an array, iterating through the window and expanding the window
            //    at the same time is not possible because the window can't be changed during iteration through the cells.
            final List<ColMajorCell> windowCells = new ArrayList<>(this.size());
            for (final Iterator<ColMajorCell> cellIter = this.iterator(); cellIter.hasNext(); )
                windowCells.add(cellIter.next());


            for (int cell = 0; cell < windowCells.size(); cell++) {
                final ColMajorCell currentCell = windowCells.get(cell);

                if ((currentCell.getCol() != minI()) && (currentCell.getRow() != maxJ()))// move to upper left if possible
                {
                    // Either extend full search radius or some fraction until edges of matrix are met.
                    final int targetCol = currentCell.getCol() - radius;
                    final int targetRow = currentCell.getRow() + radius;

                    if ((targetCol >= minI()) && (targetRow <= maxJ()))
                        markVisited(targetCol, targetRow);
                    else {
                        // Expand the window only to the edge of the matrix.
                        final int cellsPastEdge = Math.max(minI() - targetCol, targetRow - maxJ());
                        markVisited(targetCol + cellsPastEdge, targetRow - cellsPastEdge);
                    }  // end if
                }  // end if

                if (currentCell.getRow() != maxJ())  // move up if possible
                {
                    // Either extend full search radius or some fraction until edges of matrix are met.
                    final int targetCol = currentCell.getCol();
                    final int targetRow = currentCell.getRow() + radius;

                    if (targetRow <= maxJ())
                        markVisited(targetCol, targetRow);  // radius does not go past the edges of the matrix
                    else {
                        // Expand the window only to the edge of the matrix.
                        final int cellsPastEdge = targetRow - maxJ();
                        markVisited(targetCol, targetRow - cellsPastEdge);
                    }  // end if
                }  // end if

                if ((currentCell.getCol() != maxI()) && (currentCell.getRow() != maxJ())) // move to upper-right if possible
                {
                    // Either extend full search radius or some fraction until edges of matrix are met.
                    final int targetCol = currentCell.getCol() + radius;
                    final int targetRow = currentCell.getRow() + radius;

                    if ((targetCol <= maxI()) && (targetRow <= maxJ()))
                        markVisited(targetCol, targetRow);  // radius does not go past the edges of the matrix
                    else {
                        // Expand the window only to the edge of the matrix.
                        final int cellsPastEdge = Math.max(targetCol - maxI(), targetRow - maxJ());
                        markVisited(targetCol - cellsPastEdge, targetRow - cellsPastEdge);
                    }  // end if
                }  // end if

                if (currentCell.getCol() != minI())  // move left if possible
                {
                    // Either extend full search radius or some fraction until edges of matrix are met.
                    final int targetCol = currentCell.getCol() - radius;
                    final int targetRow = currentCell.getRow();

                    if (targetCol >= minI())
                        markVisited(targetCol, targetRow);  // radius does not go past the edges of the matrix
                    else {
                        // Expand the window only to the edge of the matrix.
                        final int cellsPastEdge = minI() - targetCol;
                        markVisited(targetCol + cellsPastEdge, targetRow);
                    }  // end if
                }  // end if

                if (currentCell.getCol() != maxI())  // move right if possible
                {
                    // Either extend full search radius or some fraction until edges of matrix are met.
                    final int targetCol = currentCell.getCol() + radius;
                    final int targetRow = currentCell.getRow();

                    if (targetCol <= maxI())
                        markVisited(targetCol, targetRow);  // radius does not go past the edges of the matrix
                    else {
                        // Expand the window only to the edge of the matrix.
                        final int cellsPastEdge = targetCol - maxI();
                        markVisited(targetCol - cellsPastEdge, targetRow);
                    }  // end if
                }  // end if

                if ((currentCell.getCol() != minI()) && (currentCell.getRow() != minJ()))  // move to lower-left if possible
                {
                    // Either extend full search radius or some fraction until edges of matrix are met.
                    final int targetCol = currentCell.getCol() - radius;
                    final int targetRow = currentCell.getRow() - radius;

                    if ((targetCol >= minI()) && (targetRow >= minJ()))
                        markVisited(targetCol, targetRow);  // radius does not go past the edges of the matrix
                    else {
                        // Expand the window only to the edge of the matrix.
                        final int cellsPastEdge = Math.max(minI() - targetCol, minJ() - targetRow);
                        markVisited(targetCol + cellsPastEdge, targetRow + cellsPastEdge);
                    }  // end if
                }  // end if

                if (currentCell.getRow() != minJ())  // move down if possible
                {
                    // Either extend full search radius or some fraction until edges of matrix are met.
                    final int targetCol = currentCell.getCol();
                    final int targetRow = currentCell.getRow() - radius;

                    if (targetRow >= minJ())
                        markVisited(targetCol, targetRow);  // radius does not go past the edges of the matrix
                    else {
                        // Expand the window only to the edge of the matrix.
                        final int cellsPastEdge = minJ() - targetRow;
                        markVisited(targetCol, targetRow + cellsPastEdge);
                    }  // end if
                }  // end if

                if ((currentCell.getCol() != maxI()) && (currentCell.getRow() != minJ()))  // move to lower-right if possible
                {
                    // Either extend full search radius or some fraction until edges of matrix are met.
                    final int targetCol = currentCell.getCol() + radius;
                    final int targetRow = currentCell.getRow() - radius;

                    if ((targetCol <= maxI()) && (targetRow >= minJ()))
                        markVisited(targetCol, targetRow);  // radius does not go past the edges of the matrix
                    else {
                        // Expand the window only to the edge of the matrix.
                        final int cellsPastEdge = Math.max(targetCol - maxI(), minJ() - targetRow);
                        markVisited(targetCol - cellsPastEdge, targetRow + cellsPastEdge);
                    }
                }
            }
        }
    }


    // Returns true if the window is modified.
    protected final void markVisited(int col, int row) {

        if (minValues[col] == -1)       // first value is entered in the column
        {
            minValues[col] = row;
            maxValues[col] = row;
            this.size++;
            modCount++;  // stucture has been changed
            //return true;
        } else if (minValues[col] > row)  // minimum range in the column is expanded
        {
            this.size += minValues[col] - row;
            minValues[col] = row;
            modCount++;  // stucture has been changed
        } else if (maxValues[col] < row) // maximum range in the column is expanded
        {
            this.size += row - maxValues[col];
            maxValues[col] = row;
            modCount++;
        }  // end if
    }  // end markVisited(.)


    // A private class that is a fail-fast iterator through the search window.
    private final class SearchWindowIterator implements Iterator<ColMajorCell> {
        private int currentI;
        private int currentJ;
        private final SearchWindow window;
        private boolean hasMoreElements;
        private final int expectedModCount;

        private SearchWindowIterator(SearchWindow w) {
            // Initialize values
            window = w;
            hasMoreElements = window.size() > 0;
            currentI = window.minI();
            currentJ = window.minJ();
            expectedModCount = w.modCount;
        }


        public boolean hasNext() {
            return hasMoreElements;
        }


        public ColMajorCell next() {
            if (modCount != expectedModCount)
                throw new ConcurrentModificationException();
            else if (!hasMoreElements)
                throw new NoSuchElementException();
            else {
                final ColMajorCell cell = new ColMajorCell(currentI, currentJ);
                if (++currentJ > window.maxJforI(currentI)) {
                    if (++currentI <= window.maxI())
                        currentJ = window.minJforI(currentI);
                    else
                        hasMoreElements = false;
                }
                return cell;
            }
        }


        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
