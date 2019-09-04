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



import com.hurence.logisland.timeseries.converter.common.IntList;
import com.hurence.logisland.timeseries.matrix.ColMajorCell;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.NoSuchElementException;

/**
 * @author Stan Salvador (stansalvador@hotmail.com)
 * @author f.lautenschlager
 */
public final class WarpPath {
    private final IntList tsIindexes;
    private final IntList tsJindexes;


    public WarpPath(int initialCapacity) {
        tsIindexes = new IntList(initialCapacity);
        tsJindexes = new IntList(initialCapacity);
    }

    /**
     * @return the size of the warp path
     */
    public int size() {
        return tsIindexes.size();
    }

    /**
     * @return the min i index (last element), -1 if empty
     */
    public int minI() {
        if (tsIindexes.size() > 0) {
            return tsIindexes.get(tsIindexes.size() - 1);
        }
        return -1;
    }

    /**
     * @return the min j index
     */
    public int minJ() {
        if (tsJindexes.size() > 0) {
            return tsJindexes.get(tsJindexes.size() - 1);
        }
        return -1;
    }

    /**
     * @return max i index (first element), -1 if empty
     */
    public int maxI() {
        if (tsIindexes.size() > 0) {
            return tsIindexes.get(0);
        }
        return -1;
    }

    /**
     * @return max j index (first element), -1 if empty
     */
    public int maxJ() {
        if (tsJindexes.size() > 0) {
            return tsJindexes.get(0);
        }
        return -1;
    }

    public void add(int i, int j) {
        tsIindexes.add(i);
        tsJindexes.add(j);
    }

    public ColMajorCell get(int index) {
        if ((index > this.size()) || (index < 0))
            throw new NoSuchElementException();
        else
            return new ColMajorCell(tsIindexes.get(tsIindexes.size() - index - 1), tsJindexes.get(tsJindexes.size() - index - 1));
    }


    @Override
    public String toString() {
        StringBuilder outStr = new StringBuilder("[");
        for (int x = 0; x < tsIindexes.size(); x++) {
            outStr.append("(").append(tsIindexes.get(x)).append(",").append(tsJindexes.get(x)).append(")");
            if (x < tsIindexes.size() - 1)
                outStr.append(",");
        }
        outStr.append("]");

        return outStr.toString();
    }


    @Override
    public boolean equals(Object obj) {
        if ((obj instanceof WarpPath))  // trivial false test
        {
            final WarpPath p = (WarpPath) obj;
            if ((p.size() == this.size()) && (p.maxI() == this.maxI()) && (p.maxJ() == this.maxJ())) // less trivial reject
            {
                // Compare each value in the warp path for equality
                for (int x = 0; x < this.size(); x++)
                    if (!(this.get(x).equals(p.get(x))))
                        return false;

                return true;
            } else
                return false;
        } else
            return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(tsIindexes)
                .append(tsJindexes)
                .toHashCode();
    }
}
