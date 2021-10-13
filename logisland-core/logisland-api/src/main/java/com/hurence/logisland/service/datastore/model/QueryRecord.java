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


import com.hurence.logisland.service.datastore.model.bool.BoolCondition;
import com.hurence.logisland.service.datastore.model.bool.BoolQueryRecord;
import com.hurence.logisland.service.datastore.model.bool.BoolQueryRecordRoot;

import java.util.ArrayList;
import java.util.List;

public class QueryRecord {

    private int size = -1;
    private boolean refresh = false;
    private final List<String> collections;
    private final List<String> types;
    private final BoolQueryRecordRoot boolQuery;
    private final List<SortQueryRecord> sortQueries;

    public QueryRecord() {
        this.collections = new ArrayList<>();
        this.boolQuery = new BoolQueryRecordRoot();
        this.sortQueries = new ArrayList<>();
        this.types = new ArrayList<>();
    }

    public QueryRecord addBoolQuery(BoolQueryRecord boolQuery, BoolCondition condition) {
        this.boolQuery.addBoolQuery(boolQuery, condition);
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

    public QueryRecord addCollections(List<String> collection) {
        this.collections.addAll(collection);
        return this;
    }

    public QueryRecord size(int size) {
        this.size = size;
        return this;
    }

    public List<String> getCollections() {
        return collections;
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

    public List<SortQueryRecord> getSortQueries() {
        return sortQueries;
    }

    public BoolQueryRecordRoot getBoolQuery() {
        return boolQuery;
    }
}
