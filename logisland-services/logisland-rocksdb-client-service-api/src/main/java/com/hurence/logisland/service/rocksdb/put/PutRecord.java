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
package com.hurence.logisland.service.rocksdb.put;


import com.hurence.logisland.record.Record;
import sun.awt.SunHints;

import java.util.Collection;

/**
 * Wrapper to encapsulate all of the information for the Put along with the Record.
 */
public class PutRecord {

    private final Collection<ValuePutRequest> putRequests;
    private final Record record;

    public PutRecord(Collection<ValuePutRequest> putRequests, Record record) {
        this.putRequests = putRequests;
        this.record = record;
    }

    public Collection<ValuePutRequest> getPutRequests() {
        return putRequests;
    }

    public Record getRecord() {
        return record;
    }

    public boolean isValid() {
        if (record == null || putRequests == null || putRequests.isEmpty()) {
            return false;
        }

        for (ValuePutRequest putRequest : putRequests) {
            if (null == putRequest.getFamily() || null == putRequest.getKey() || putRequest.getValue() == null) {
                return false;
            }
        }

        return true;
    }

}
