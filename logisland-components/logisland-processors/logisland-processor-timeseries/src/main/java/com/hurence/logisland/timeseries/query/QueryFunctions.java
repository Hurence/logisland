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
package com.hurence.logisland.timeseries.query;


import com.hurence.logisland.timeseries.functions.ChronixAggregation;
import com.hurence.logisland.timeseries.functions.ChronixAnalysis;
import com.hurence.logisland.timeseries.functions.ChronixEncoding;
import com.hurence.logisland.timeseries.functions.ChronixTransformation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class that holds the query functions
 * - aggregations
 * - analyses
 * - transformations
 *
 * @author f.lautenschlager
 */
class QueryFunctions {

    private List<ChronixAnalysis> analyses;
    private List<ChronixAggregation> aggregations;
    private List<ChronixTransformation> transformations;
    private List<ChronixEncoding> encodings;

    QueryFunctions() {
        analyses = new ArrayList<>();
        aggregations = new ArrayList<>();
        transformations = new ArrayList<>();
        encodings = new ArrayList<>();
    }

    /**
     * @return the analyses in the query
     */
    public List<ChronixAnalysis> getAnalyses() {
        return analyses;
    }

    /**
     * @return the aggregations in the query
     */
    public List<ChronixAggregation> getAggregations() {
        return aggregations;
    }

    /**
     * @return the transformations in the query
     */
    public List<ChronixTransformation> getTransformations() {
        return transformations;
    }

    /**
     * @return the encodings in the query
     */
    public List<ChronixEncoding> getEncodings() {
        return encodings;
    }

    /**
     * Add the given analysis to the query functions
     *
     * @param analysis the analysis
     */
    public void addAnalysis(ChronixAnalysis analysis) {
        this.analyses.add(analysis);
    }

    /**
     * Add the given aggregation to the query functions
     *
     * @param aggregation the aggregation
     */
    public void addAggregation(ChronixAggregation aggregation) {
        this.aggregations.add(aggregation);
    }

    /**
     * Add the given transformation to the query functions
     *
     * @param transformation the transformation
     */
    public void addTransformation(ChronixTransformation transformation) {
        this.transformations.add(transformation);
    }

    /**
     * Add the given encoding to the query functions
     *
     * @param encoding the encoding
     */
    public void addEncoding(ChronixEncoding encoding) {
        this.encodings.add(encoding);
    }


    /**
     * @return true if all (aggregations, analyses, transformations) are emtpy, otherwise false
     */
    public boolean isEmpty() {
        return transformations.isEmpty() && aggregations.isEmpty() && analyses.isEmpty() && encodings.isEmpty();
    }

    /**
     * @return true if the functions contains transformations
     */
    public boolean containsTransformations() {
        return !transformations.isEmpty();
    }

    /**
     * @return true if the functions contains aggregations
     */
    public boolean containsAggregations() {
        return !aggregations.isEmpty();
    }

    /**
     * @return true if the functions contains encodings
     */
    public boolean containsEncodings() {
        return !encodings.isEmpty();
    }

    /**
     * @return true if the functions contains analyses
     */
    public boolean containsAnalyses() {
        return !analyses.isEmpty();
    }

    /**
     * @return the size of all (aggregations, analyses, transformations)
     */
    public int size() {
        return sizeOfAggregations() + sizeOfAnalyses() + sizeOfTransformations() + sizeOfEncodings();
    }

    /**
     * @return the size of the transformations
     */
    public int sizeOfTransformations() {
        return transformations.size();
    }

    /**
     * @return the amount of aggregations
     */
    public int sizeOfAggregations() {
        return aggregations.size();
    }

    /**
     * @return the size of analyses
     */
    public int sizeOfAnalyses() {
        return analyses.size();
    }

    /**
     * @return the size of encodings
     */
    public int sizeOfEncodings() {
        return encodings.size();
    }

    /**
     * @param functions the other query functions
     */
    public void merge(QueryFunctions functions) {
        aggregations.addAll(functions.aggregations);
        transformations.addAll(functions.transformations);
        analyses.addAll(functions.analyses);
        encodings.addAll(functions.encodings);
    }
}
