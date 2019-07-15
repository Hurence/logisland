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
package com.hurence.logisland.timeseries.functions;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Simple fixed size map of chronix analysis and value
 *
 * @author f.lautenschlager
 */
public class FunctionValueMap implements Serializable {

    private final ChronixAnalysis[] analyses;
    private final ChronixTransformation[] transformations;
    private final ChronixAggregation[] aggregations;
    private final ChronixEncoding[] encodings;

    private final boolean[] analysisValues;
    private final double[] aggregationValues;
    private final String[] encodingValues;

    private final String[] identifiers;

    private int analysisSize;
    private int aggregationSize;
    private int transformationSize;
    private int encodingSize;


    /**
     * A container for analyses and its results
     *
     * @param amountOfAggregations    the number of aggregations
     * @param amountOfAnalyses        the number of analyses
     * @param amountOfTransformations the number of transformations
     */
    public FunctionValueMap(int amountOfAggregations, int amountOfAnalyses, int amountOfTransformations) {
        this.aggregations = new ChronixAggregation[amountOfAggregations];
        this.analyses = new ChronixAnalysis[amountOfAnalyses];
        this.transformations = new ChronixTransformation[amountOfTransformations];
        this.encodings = new ChronixEncoding[0];

        this.analysisValues = new boolean[amountOfAnalyses];
        this.aggregationValues = new double[amountOfAggregations];
        this.encodingValues  = new String[0];

        this.identifiers = new String[amountOfAggregations + amountOfAnalyses];
    }


    /**
     * A container for analyses and its results
     *
     * @param amountOfAggregations    the number of aggregations
     * @param amountOfAnalyses        the number of analyses
     * @param amountOfTransformations the number of transformations
     * @param amountOfEncodings       the number of encodings
     */
    public FunctionValueMap(int amountOfAggregations, int amountOfAnalyses, int amountOfTransformations, int amountOfEncodings) {
        this.aggregations = new ChronixAggregation[amountOfAggregations];
        this.analyses = new ChronixAnalysis[amountOfAnalyses];
        this.transformations = new ChronixTransformation[amountOfTransformations];
        this.encodings = new ChronixEncoding[amountOfEncodings];

        this.analysisValues = new boolean[amountOfAnalyses];
        this.aggregationValues = new double[amountOfAggregations];
        this.encodingValues  = new String[amountOfEncodings];

        this.identifiers = new String[amountOfAggregations + amountOfAnalyses];
    }

    /**
     * Appends the analysis to the result
     *
     * @param analysis the chronix analysis
     * @param value    the value for the analysis
     */
    public void add(ChronixAnalysis analysis, boolean value, String identifier) {
        if (analysisSize < analyses.length) {
            analyses[analysisSize] = analysis;
            analysisValues[analysisSize] = value;
            identifiers[analysisSize] = identifier;
            analysisSize++;
        } else {
            throw new IndexOutOfBoundsException("Try to put analysis to map with max size " + analyses.length + " but index " + analysisSize + " is out of range.");
        }
    }

    /**
     * Append the aggregation to the result
     *
     * @param aggregation the chronix aggregation
     * @param value       the value of the aggregation
     */
    public void add(ChronixAggregation aggregation, double value) {
        if (aggregationSize < aggregations.length) {
            aggregations[aggregationSize] = aggregation;
            aggregationValues[aggregationSize] = value;
            aggregationSize++;
        } else {
            throw new IndexOutOfBoundsException("Try to put aggregation to map with max size " + aggregations.length + " but index " + aggregationSize + " is out of range.");
        }
    }

    /**
     * Append the encoding to the result
     *
     * @param encoding the chronix encoding
     * @param value    the value of the encoding
     */
    public void add(ChronixEncoding encoding, String value) {
        if (encodingSize < encodings.length) {
            encodings[encodingSize] = encoding;
            encodingValues[encodingSize] = value;
            encodingSize++;
        } else {
            throw new IndexOutOfBoundsException("Try to put encoding to map with max size " + encodings.length + " but index " + encodingSize + " is out of range.");
        }
    }

    /**
     * Appends the transformation to the result
     *
     * @param transformation add an transformation
     */
    public void add(ChronixTransformation transformation) {
        if (transformationSize < transformations.length) {
            transformations[transformationSize] = transformation;
            transformationSize++;
        } else {
            throw new IndexOutOfBoundsException("Try to put transformation to map with max size " + transformations.length + " but index " + transformationSize + " is out of range.");
        }

    }

    /**
     * Gets the transformation
     *
     * @param i the index
     * @return the transformation at index i
     */
    public ChronixTransformation getTransformation(int i) {
        return transformations[i];
    }

    /**
     * Gets the analysis at the index position
     *
     * @param i the index
     * @return the analysis at index i
     */
    public ChronixAnalysis getAnalysis(int i) {
        return analyses[i];
    }

    /**
     * Gets the analysis value at the index position
     *
     * @param i the index
     * @return the value at index i
     */
    public boolean getAnalysisValue(int i) {
        return analysisValues[i];
    }

    /**
     * Gets the aggregation at the index position
     *
     * @param i the index
     * @return the aggregation at index i
     */
    public ChronixAggregation getAggregation(int i) {
        return aggregations[i];
    }


    /**
     * Gets the encoding at the index position
     *
     * @param i the index
     * @return the encoding at index i
     */
    public ChronixEncoding getEncoding(int i) {
        return encodings[i];
    }

    /**
     * Gets the aggregation value at the index position
     *
     * @param i the index
     * @return the value at index i
     */
    public double getAggregationValue(int i) {
        return aggregationValues[i];
    }

    /**
     * Gets the encoding value at the index position
     *
     * @param i the index
     * @return the value at index i
     */
    public String getEncodingValue(int i) {
        return encodingValues[i];
    }

    /**
     * Gets the identifier used in analyses that need sub queries
     *
     * @param i the index
     * @return the additional identifier
     */
    public String getAnalysisIdentifier(int i) {
        return identifiers[i];
    }

    /**
     * @return the size of the analyses
     */
    public int sizeOfAnalyses() {
        return analysisSize;
    }

    /**
     * @return the size of the encodings
     */
    public int sizeOfEncodings() {
        return encodingSize;
    }

    /**
     * @return the size of the transformations
     */
    public int sizeOfTransformations() {
        return transformationSize;
    }

    /**
     * @return the total amount of functions
     */
    public int size() {
        return analysisSize + transformationSize + aggregationSize;
    }

    /**
     * @return the size of the aggregations
     */
    public int sizeOfAggregations() {
        return aggregationSize;
    }

    @Override
    public String toString() {
        return "FunctionValueMap{" +
                "analyses=" + Arrays.toString(analyses) +
                ", transformations=" + Arrays.toString(transformations) +
                ", aggregations=" + Arrays.toString(aggregations) +
                ", analysisValues=" + Arrays.toString(analysisValues) +
                ", aggregationValues=" + Arrays.toString(aggregationValues) +
                ", identifiers=" + Arrays.toString(identifiers) +
                ", analysisSize=" + analysisSize +
                ", aggregationSize=" + aggregationSize +
                ", transformationSize=" + transformationSize +
                '}';
    }
}
