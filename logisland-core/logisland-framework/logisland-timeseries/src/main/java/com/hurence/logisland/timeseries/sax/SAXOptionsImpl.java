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
package com.hurence.logisland.timeseries.sax;

public class SAXOptionsImpl implements SAXOptions {

    private int paaSize;
    private double nThreshold;
    private int alphabetSize;

    public SAXOptionsImpl(int paaSize, double nThreshold, int alphabetSize) {
        this.paaSize = paaSize;
        this.nThreshold = nThreshold;
        this.alphabetSize = alphabetSize;
    }

    @Override
    public int paaSize() {
        return paaSize;
    }

    @Override
    public double nThreshold() {
        return nThreshold;
    }

    @Override
    public int alphabetSize() {
        return alphabetSize;
    }
}
