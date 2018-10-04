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
package com.hurence.logisland.sampling;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

import java.io.Serializable;

public abstract class AbstractSampler implements Sampler, Serializable {


    protected String valueFieldName;
    protected String timeFieldName;

    public AbstractSampler(String valueFieldName, String timeFieldName) {
        this.valueFieldName = valueFieldName;
        this.timeFieldName = timeFieldName;
    }

    public AbstractSampler(String valueFieldName) {
        this.valueFieldName = valueFieldName;
        this.timeFieldName = FieldDictionary.RECORD_TIME;
    }

    public AbstractSampler() {
        this.valueFieldName = FieldDictionary.RECORD_VALUE;
        this.timeFieldName = FieldDictionary.RECORD_TIME;
    }

    public Double getRecordValue(Record record){
        if (record.hasField(valueFieldName)) {
            return record.getField(valueFieldName).asDouble();
        }else{
            return null;
        }
    }

    public Long getRecordTime(Record record){
        if (record.hasField(timeFieldName)) {
            return record.getField(timeFieldName).asLong();
        }else{
            return null;
        }
    }

    /**
     * retrun the same record as input by keeping only time and value fields.
     *
     * @param record
     * @return
     */
    public Record getTimeValueRecord(Record record){
        Record tvRecord = new StandardRecord(record.getType());
        Double value = getRecordValue(record);
        if(value != null)
            tvRecord.setField(valueFieldName, record.getField(valueFieldName).getType(), value);

        Long time = getRecordTime(record);
        if(time != null)
            tvRecord.setField(timeFieldName, record.getField(timeFieldName).getType(), time);

        return tvRecord;
    }
}
