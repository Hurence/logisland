package com.hurence.logisland.webanalytics.test.util;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * No thread safe
 */
public class EventsGenerator {

    public final static String SESSION_ID = "sessionId";
    public final static String TOPIC_COLUMN = "topic";
    public final static String TIMESTAMP = "timestamp";
    public final static String FROM_TOPIC = "fromTopic";

    final String sessionId;

    private long eventId = 0;

    public EventsGenerator(String sessionId) {
        this.sessionId = sessionId;
    }

    public Record generateEvent(long timestamp, String outputTopic) {
        return generateEvent(timestamp, outputTopic, "unknown");
    }


    public Record generateEvent(long timestamp, String outputTopic, String fromTopic) {
        Record record = new StandardRecord("generated");
        record.setId(sessionId + String.valueOf(eventId++));
        record.setStringField(SESSION_ID, sessionId);
        record.setStringField(TOPIC_COLUMN, outputTopic);
        record.setLongField(TIMESTAMP, timestamp);
        record.setStringField(FROM_TOPIC, fromTopic);
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
