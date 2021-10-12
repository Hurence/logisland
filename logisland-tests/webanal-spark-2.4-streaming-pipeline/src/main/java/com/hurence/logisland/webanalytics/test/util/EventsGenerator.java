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
package com.hurence.logisland.webanalytics.test.util;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class EventsGenerator {

    final String sessionId;

    public EventsGenerator(String sessionId) {
        this.sessionId = sessionId;
    }

    public Record generateEvent(long timestamp, String url) {
        Record record = new StandardRecord("generated");
//        record.setStringField(TestMappings.eventsInternalFields.getSessionIdField(), sessionId);
//        record.setLongField(TestMappings.eventsInternalFields.getTimestampField(), timestamp);
//        record.setStringField(TestMappings.eventsInternalFields.getVisitedPageField(), url);
        record.setStringField("sessionId", sessionId);
        record.setLongField("ts", timestamp);
        record.setStringField("TestMappings.eventsInternalFields.getVisitedPageField()", url);
        return record;
    }

    public List<Record> generateEvents(List<Long> timestamps,
                                       String url) {
        return timestamps.stream()
                .map(ts -> generateEvent(ts, url))
                .collect(Collectors.toList());
    }

    public List<Record> generateEvents(Long from,
                                       Long to,
                                       Long padding) {
        int numberOfRecord = (int)((to - from) / padding);
        return LongStream.iterate(from, ts -> ts + padding)
                .limit(numberOfRecord)
                .mapToObj(ts -> generateEvent(ts, "url"))
                .collect(Collectors.toList());
    }

    public List<Record> generateEventsRandomlyOrdered(Long from,
                                       Long to,
                                       Long padding) {
        List<Record> sortedList = generateEvents(from, to, padding);
        Collections.shuffle(sortedList);;
        return sortedList;
    }
}
