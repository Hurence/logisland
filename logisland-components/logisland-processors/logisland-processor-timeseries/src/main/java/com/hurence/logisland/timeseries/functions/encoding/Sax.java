/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.logisland.timeseries.functions.encoding;


import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.functions.ChronixAggregation;
import com.hurence.logisland.timeseries.functions.ChronixEncoding;
import com.hurence.logisland.timeseries.functions.FunctionValueMap;
import net.seninp.jmotif.sax.NumerosityReductionStrategy;
import net.seninp.jmotif.sax.SAXException;
import net.seninp.jmotif.sax.SAXProcessor;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;
import net.seninp.jmotif.sax.datastructure.SAXRecords;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Set;

/**
 * @author bailett
 * <p>
 * In short, Symbolic Aggregate approXimation (SAX) algorithm application to the input time series transforms
 * its into a strings.
 * <p>
 * The algoithm was proposed by Lin et al.) and extends the PAA-based approach inheriting the original algorithm
 * simplicity and low computational complexity while providing satisfactory sensitivity and selectivity
 * in range query processing. Moreover, the use of a symbolic representation opened a door to the existing wealth
 * of data-structures and string-manipulation algorithms in computer science such as hashing, regular expression,
 * pattern matching, suffix trees, and grammatical inference.
 */
public final class Sax implements ChronixEncoding<MetricTimeSeries> {


    private final int alphabetSize;
    private final float nThreshold;
    private final int paaSize;


    /**
     * Constructs a Sax transformation without sliding windows
     *
     * @param args the first value is the alphabet size, the second is the nThreshold, the third is paaSize
     */
    public Sax(String[] args) {
        alphabetSize = Integer.parseInt(args[0]);
        nThreshold = Float.parseFloat(args[1]);
        paaSize = Integer.parseInt(args[2]);
    }


    /**
     * Default Sax transformation
     */
    public Sax() {
        alphabetSize = 3;
        nThreshold = 0.01f;
        paaSize = 4;
    }


    @Override
    public void execute(MetricTimeSeries timeSeries, FunctionValueMap functionValueMap) {

        if (timeSeries.isEmpty()) {
            functionValueMap.add(this, "");
            return;
        }

        // instantiate classes
        NormalAlphabet na = new NormalAlphabet();
        SAXProcessor sp = new SAXProcessor();

        double[] ts = timeSeries.getValuesAsArray();


        try {
            // perform the discretization
            SAXRecords res = sp.ts2saxByChunking(ts, paaSize, na.getCuts(alphabetSize), nThreshold);
            functionValueMap.add(this, res.getSAXString(""));
        } catch (SAXException e) {
            e.printStackTrace();
        }


    }

    @Override
    public String getQueryName() {
        return "sax";
    }

    @Override
    public String getTimeSeriesType() {
        return "metric";
    }

    @Override
    public String[] getArguments() {
        return new String[]{"alphabetSize=" + alphabetSize,
                "nThreshold=" + nThreshold,
                "paaSize=" + paaSize};
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return new EqualsBuilder()
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .toHashCode();
    }

}
