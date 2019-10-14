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
package com.hurence.logisland.timeseries.sampling;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.util.List;
import java.util.stream.Collectors;

public class AverageSampler extends AbstractSampler {


    private int bucketSize;


    public AverageSampler(String valueFieldName, String timeFieldName, int bucketSize) {
        super(valueFieldName, timeFieldName);
        this.bucketSize = bucketSize;
    }


    /**
     * divide the points sequence into equally sized buckets
     * and select the first point of each bucket
     *
     * @param inputRecords the iput list
     * @return
     */
    @Override
    public List<Record> sample(List<Record> inputRecords) {


        SummaryStatistics stats = new SummaryStatistics();


        // simple average to 100 data points
        final int realBucketSize = SamplingUtils.fitBucketSize(inputRecords, bucketSize);
        return SamplingUtils.grouped(inputRecords, realBucketSize)
                .map(bucket -> {

                    bucket.forEach(record -> {
                        final Double recordValue = getRecordValue(record);
                        if (recordValue != null)
                            stats.addValue(recordValue);
                    });

                    final double meanValue = stats.getMean();
                    final Record sampleRecord = getTimeValueRecord(bucket.get(0));
                    final FieldType fieldType = bucket.get(0).getField(valueFieldName).getType();
                    switch (fieldType) {
                        case INT:
                            sampleRecord.setField(valueFieldName, fieldType, (int) Math.round(meanValue));
                            break;
                        case LONG:
                            sampleRecord.setField(valueFieldName, fieldType, Math.round(meanValue));
                            break;
                        case FLOAT:
                            sampleRecord.setField(valueFieldName, fieldType, (float) meanValue);
                            break;
                        case DOUBLE:
                            sampleRecord.setField(valueFieldName, fieldType, meanValue);
                            break;
                    }
                    return sampleRecord;
                }).collect(Collectors.toList());
    }
}
