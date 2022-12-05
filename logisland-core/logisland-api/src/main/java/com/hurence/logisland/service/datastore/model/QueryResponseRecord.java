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
package com.hurence.logisland.service.datastore.model;

import java.util.ArrayList;
import java.util.List;

public class QueryResponseRecord {

    private final long totalMatched;
    private final List<ResponseRecord> docs;
    private final List<AggregationResponseRecord> aggregations;


    public QueryResponseRecord(long totalMatched, List<ResponseRecord> docs) {
        this.totalMatched = totalMatched;
        this.docs = docs;
        this.aggregations = new ArrayList<>();
    }

    public QueryResponseRecord(long totalMatched, List<ResponseRecord> docs, List<AggregationResponseRecord> aggregations) {
        this.totalMatched = totalMatched;
        this.docs = docs;
        this.aggregations = aggregations;
    }

    public long getTotalMatched() {
        return totalMatched;
    }

    public List<ResponseRecord> getDocs() {
        return docs;
    }

    public List<AggregationResponseRecord> getAggregations() {
        return aggregations;
    }

    @Override
    public String toString() {
        return "QueryResponseRecord{" +
                "totalMatched=" + totalMatched +
                ", docs=" + docs +
                '}';
    }
}
