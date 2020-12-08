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
package com.hurence.logisland.service.datastore;


import java.util.ArrayList;
import java.util.List;

public class QueryRecord {

    private int size = -1;
    private boolean refresh = false;
    private final List<String> collections;
    private final List<String> types;
    private final List<TermQueryRecord> termQueries;
    private final List<WildCardQueryRecord> wildCardQueries;
    private final List<RangeQueryRecord> rangeQueries;
    private final List<SortQueryRecord> sortQueries;

    public QueryRecord() {
        this.collections = new ArrayList<>();
        this.termQueries = new ArrayList<>();
        this.rangeQueries = new ArrayList<>();
        this.wildCardQueries = new ArrayList<>();
        this.sortQueries = new ArrayList<>();
        this.types = new ArrayList<>();
    }

    public QueryRecord addTermQuery(TermQueryRecord termQuery) {
        this.termQueries.add(termQuery);
        return this;
    }

    public QueryRecord addRangeQuery(RangeQueryRecord termQuery) {
        this.rangeQueries.add(termQuery);
        return this;
    }

    public QueryRecord addWildCardQuery(WildCardQueryRecord wildCardQuery) {
        this.wildCardQueries.add(wildCardQuery);
        return this;
    }

    public QueryRecord addSortQuery(SortQueryRecord sortQuery) {
        this.sortQueries.add(sortQuery);
        return this;
    }

    public QueryRecord addCollection(String collection) {
        this.collections.add(collection);
        return this;
    }

    public QueryRecord size(int size) {
        this.size = size;
        return this;
    }

    @Override
    public String toString() {
        return termQueries.toString();
    }

    public List<String> getCollections() {
        return collections;
    }

    public List<TermQueryRecord> getTermQueries() {
        return termQueries;
    }

    public List<RangeQueryRecord> getRangeQueries() {
        return rangeQueries;
    }

    public boolean getRefresh() {
        return refresh;
    }

    public QueryRecord setRefresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public QueryRecord addType(String type) {
        this.types.add(type);
        return this;
    }

    public int getSize() {
        return size;
    }

    public List<String> getTypes() {
        return types;
    }

    public List<WildCardQueryRecord> getWildCardQueries() {
        return wildCardQueries;
    }

    public List<SortQueryRecord> getSortQueries() {
        return sortQueries;
    }
}
