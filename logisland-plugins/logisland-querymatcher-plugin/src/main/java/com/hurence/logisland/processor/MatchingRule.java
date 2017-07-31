/**
 * Copyright (C) 2016-2017 Hurence (support@hurence.com)
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
package com.hurence.logisland.processor;

/**
 * Created by fprunier on 15/04/16.
 */
class MatchingRule {
    private final String name;
    private final String query;
    private final String legacyQuery;


    public MatchingRule(final String name, final String query) {
        this.name = name;
        this.query = query;
        this.legacyQuery = this.query;
    }

    public MatchingRule(final String name, final String revisitedQuery, final String legacyQuery) {
        this.name = name;
        this.query = revisitedQuery;
        this.legacyQuery = legacyQuery;
    }

    public String getName() {
        return name;
    }

    public String getQuery() {
        return query;
    }

    public String getLegacyQuery() {
        return legacyQuery;
    }
}
