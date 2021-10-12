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
package com.hurence.logisland.service.datastore.model.bool;

public class RangeQueryRecord implements BoolQueryRecord {
    public static final boolean DEFAULT_INCLUDE_UPPER = true;
    public static final boolean DEFAULT_INCLUDE_LOWER = true;

    private String fieldName;
    private Object from = null;
    private Object to = null;
    private boolean includeLower = DEFAULT_INCLUDE_LOWER;
    private boolean includeUpper = DEFAULT_INCLUDE_UPPER;

    public RangeQueryRecord(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public RangeQueryRecord setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public Object getFrom() {
        return from;
    }

    /**
     * set min
     * @param from
     * @return
     */
    public RangeQueryRecord setFrom(Object from) {
        this.from = from;
        return this;
    }


    public Object getTo() {
        return to;
    }

    /**
     * set max
     * @param to
     * @return
     */
    public RangeQueryRecord setTo(Object to) {
        this.to = to;
        return this;
    }

    public boolean isIncludeLower() {
        return includeLower;
    }

    /**
     * include min ?
     * @param includeLower
     * @return
     */
    public RangeQueryRecord setIncludeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    public boolean isIncludeUpper() {
        return includeUpper;
    }

    /**
     * include max ?
     * @param includeUpper
     * @return
     */
    public RangeQueryRecord setIncludeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    @Override
    public String toString() {
        return "RangeQueryRecord{" +
                "fieldName='" + fieldName + '\'' +
                ", from=" + from +
                ", to=" + to +
                ", includeLower=" + includeLower +
                ", includeUpper=" + includeUpper +
                '}';
    }
}
