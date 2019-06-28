package com.hurence.logisland.timeseries.converter;

import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.*;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
import com.hurence.logisland.timeseries.dts.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RecordsTimeSeriesConverter implements Serializable {


    private Logger logger = LoggerFactory.getLogger(RecordsTimeSeriesConverter.class.getName());
    private MetricTimeSeriesConverter converter = new MetricTimeSeriesConverter();
    private boolean saxEncoding;
    private int ddcThreshold = 0;


    public RecordsTimeSeriesConverter(boolean saxEncoding) {
        this.saxEncoding = saxEncoding;
    }

    public RecordsTimeSeriesConverter() {
        this(false);
    }

    /**
     * Compact a related list of records a single chunked one
     *
     * @param records
     * @return
     * @throws ProcessException
     */
    public TimeSerieChunkRecord chunk(List<Record> records) throws ProcessException {
        //extract metricType
        String metricType = records.stream().filter(record -> record.hasField(FieldDictionary.RECORD_TYPE) &&
                record.getField(FieldDictionary.RECORD_TYPE).getRawValue() != null)
                .map(record -> record.getField(FieldDictionary.RECORD_TYPE).asString())
                .findFirst().orElse(RecordDictionary.METRIC);
        //extract metricName
        String metricName = records.stream().filter(record -> record.hasField(FieldDictionary.RECORD_NAME) &&
                record.getField(FieldDictionary.RECORD_NAME).getRawValue() != null)
                .map(record -> record.getField(FieldDictionary.RECORD_NAME).asString())
                .findFirst().orElse("unknown");
        final TimeSerieChunkRecord chunkrecord = new TimeSerieChunkRecord(metricType, metricName);
        //first TS
        final long firstTS = getFirstTS(records);
        chunkrecord.setStart(firstTS);
        //last TS
        long tmp = getLastTS(records);
        final long lastTS = tmp == firstTS ? firstTS + 1 : tmp;
        chunkrecord.setEnd(lastTS);
        //compress chunk
        chunkrecord.setCompressedPoints(compressPoints(records.stream()));
        //set attributes
        Record first = records.get(0);
        first.getAllFieldsSorted().forEach(field -> {
                    chunkrecord.addAttributes(field.getName(), field.getRawValue());
                });
        //TODO add sax encoding
        return chunkrecord;
    }

    private long getLastTS(List<Record> records) {
        return records.get(records.size() - 1).getTime().getTime();

    }

    private long getFirstTS(List<Record> records) {
        return records.get(0).getTime().getTime();
    }

    private byte[] compressPoints(Stream<Record> records) {
        Stream<Point> points = records
                .filter(record -> record.getField(FieldDictionary.RECORD_VALUE) != null && record.getField(FieldDictionary.RECORD_VALUE).getRawValue() != null)
                .map(record -> new Pair<>(record.getTime().getTime(), record.getField(FieldDictionary.RECORD_VALUE).asDouble()))
                .filter(longDoublePair -> longDoublePair.getSecond() != null && Double.isFinite(longDoublePair.getSecond()))
                .map(pair -> new Point(0 ,pair.getFirst(), pair.getSecond()));

        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }

    /**
     * Reverse operation for chunk operation
     *
     * @param record
     * @return
     * @throws ProcessException
     */
    public List<Record> unchunk(final TimeSerieChunkRecord record) throws IOException {

        final String name = record.getMetricName();
        final long start = record.getStart();
        final long end = record.getEnd();
        final String type = record.getMetricType();

        return unCompressPoints(record.getCompressedPoints(), start, end).stream()
                .map(m -> {
                    long recordTime = m.getTimestamp();
                    double recordValue = m.getValue();
                    //TODO implement buildPatternForSetter implying modifying Record interface
                    TimeSeriePointRecord pointRecord = new TimeSeriePointRecord(type, name);
                    pointRecord.setTimestamp(recordTime);
                    pointRecord.setValue(recordValue);
                    return pointRecord;
//                    return new TimeSeriePointRecord(type, name)
//                            .setTimestamp(recordTime)
//                            .setValue(recordValue);
                }).collect(Collectors.toList());
    }

    private List<Point> unCompressPoints(byte[] chunkOfPoints, long start, long end) throws IOException {
        try (InputStream decompressed = Compression.decompressToStream(chunkOfPoints)) {
            return ProtoBufMetricTimeSeriesSerializer.from(decompressed, start, end);
        }
    }

}
