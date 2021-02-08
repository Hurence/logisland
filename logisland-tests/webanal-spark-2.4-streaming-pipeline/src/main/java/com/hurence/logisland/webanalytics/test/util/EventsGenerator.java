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
