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


import java.util.List;
import java.util.stream.Collectors;

public class MultiQueryResponseRecord {

    private final List<QueryResponseRecord> responses;

    public MultiQueryResponseRecord(List<QueryResponseRecord> responses) {
        this.responses = responses;
    }

    public long getTotalMatched() {
        return responses.stream()
                .map(QueryResponseRecord::getTotalMatched)
                .reduce(0L, Long::sum);
    }

    public List<QueryResponseRecord> getResponses() {
        return responses;
    }

    public List<ResponseRecord> getDocs() {
        return responses.stream()
                .map(QueryResponseRecord::getDocs)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "MultiQueryResponseRecord{" +
                "responses=" + responses +
                '}';
    }
}
