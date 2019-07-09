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
import com.hurence.logisland.timeseries.functions.ChronixEncoding;
import com.hurence.logisland.timeseries.functions.FunctionValueMap;
import net.seninp.jmotif.sax.NumerosityReductionStrategy;
import net.seninp.jmotif.sax.SAXException;
import net.seninp.jmotif.sax.SAXProcessor;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;
import net.seninp.jmotif.sax.datastructure.SAXRecords;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

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
public final class SaxSliding implements ChronixEncoding<MetricTimeSeries> {


    private final int alphabetSize;
    private final NumerosityReductionStrategy nrStrategy;
    private final int numThreads;
    private final float nThreshold;
    private final int slidingWindowSize;
    private final int paaSize;

    /**
     * Constructs a Sax transformation with sliding windows
     *
     * @param args the first value is the alphabet size, the second is the NumerosityReductionStrategy,
     *             the third is numThreads, the fourth nThreshold, the fifth slidingWindowSize and
     *             the sixth is paaSize
     */
    public SaxSliding(String[] args) {
        alphabetSize = Integer.parseInt(args[0]);
        nrStrategy = NumerosityReductionStrategy.valueOf(args[1]);
        numThreads = Integer.parseInt(args[2]);
        nThreshold = Float.parseFloat(args[3]);
        slidingWindowSize = Integer.parseInt(args[4]);
        paaSize = Integer.parseInt(args[5]);
    }



    /**
     * Default Sax transformation
     */
    public SaxSliding() {
        alphabetSize = 3;
        nrStrategy = NumerosityReductionStrategy.EXACT;
        numThreads = 1;
        nThreshold = 0.01f;
        slidingWindowSize = 30;
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
           SAXRecords res = sp.ts2saxViaWindowGlobalZNorm(ts, slidingWindowSize, paaSize,
                    na.getCuts(alphabetSize), nrStrategy, nThreshold);

            functionValueMap.add(this, res.getSAXString(""));

        } catch (SAXException e) {
            e.printStackTrace();
        }


    }

    @Override
    public String getQueryName() {
        return "saxsl";
    }

    @Override
    public String getTimeSeriesType() {
        return "metric";
    }

    @Override
    public String[] getArguments() {
        return new String[]{"alphabetSize=" + alphabetSize,
                "nrStrategy=" + nrStrategy.name().toUpperCase(),
                "numThreads=" + numThreads,
                "nThreshold=" + nThreshold,
                "slidingWindowSize=" + slidingWindowSize,
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
