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
package com.hurence.logisland.timeseries.sampling.record;

import com.hurence.logisland.record.Record;

import java.util.List;
import java.util.stream.Collectors;


public class IsoRecordSampler extends AbstractRecordSampler {

    public IsoRecordSampler(String valueFieldName, String timeFieldName) {
        super(valueFieldName, timeFieldName);
    }

    /**
     * do no sample at all => for test or benchmark purpose
     *
     * @param inputRecords
     * @return the same list as input
     */
    @Override
    public List<Record> sample(List<Record> inputRecords) {
        return inputRecords
                .stream()
                .map(this::getTimeValueRecord)
                .collect(Collectors.toList());
    }
}
