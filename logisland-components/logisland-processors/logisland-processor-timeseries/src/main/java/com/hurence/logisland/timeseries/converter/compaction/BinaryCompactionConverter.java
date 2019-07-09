package com.hurence.logisland.timeseries.converter.compaction;

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

public class BinaryCompactionConverter implements Serializable {


    private static Logger logger = LoggerFactory.getLogger(BinaryCompactionConverter.class.getName());

    private int ddcThreshold = 0;//Try to vary this

    private BinaryCompactionConverter(int ddcThreshold) {
        this.ddcThreshold = ddcThreshold;
    }

    private BinaryCompactionConverter() {}

    //TODO reduce to compaction only
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
        //set attributes
        Record first = records.get(0);
        first.getAllFieldsSorted().forEach(field -> {
            chunkrecord.addAttributes(field.getName(), field.getRawValue());
        });
        //find points
        List<Point> points = extractPoints(records.stream()).collect(Collectors.toList());
        //compress chunk into binaries
        chunkrecord.setCompressedPoints(compressPoints(points.stream()));
        return chunkrecord;
    }

    private long getLastTS(List<Record> records) {
        return records.get(records.size() - 1).getTime().getTime();

    }

    private long getFirstTS(List<Record> records) {
        return records.get(0).getTime().getTime();
    }

    private byte[] compressPoints(Stream<Point> points) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }

    private Stream<Point> extractPoints(Stream<Record> records) {
        return records
                .filter(record -> record.getField(FieldDictionary.RECORD_VALUE) != null && record.getField(FieldDictionary.RECORD_VALUE).getRawValue() != null)
                .map(record -> new Pair<>(record.getTime().getTime(), record.getField(FieldDictionary.RECORD_VALUE).asDouble()))
                .filter(longDoublePair -> longDoublePair.getSecond() != null && Double.isFinite(longDoublePair.getSecond()))
                .map(pair -> new Point(0 ,pair.getFirst(), pair.getSecond()));
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
                    TimeSeriePointRecord pointRecord = new TimeSeriePointRecord(type, name);
                    pointRecord.setTimestamp(recordTime);
                    pointRecord.setValue(recordValue);
                    return pointRecord;
                }).collect(Collectors.toList());
    }

    private List<Point> unCompressPoints(byte[] chunkOfPoints, long start, long end) throws IOException {
        try (InputStream decompressed = Compression.decompressToStream(chunkOfPoints)) {
            return ProtoBufMetricTimeSeriesSerializer.from(decompressed, start, end);
        }
    }



    public static final class Builder {

        private int ddcThreshold = 0;

        public Builder ddcThreshold(final int ddcThreshold) {
            this.ddcThreshold = ddcThreshold;
            return this;
        }
        /**
         * @return a BinaryCompactionConverter as configured
         *
         */
        public BinaryCompactionConverter build() {
           return new BinaryCompactionConverter(ddcThreshold);
        }
    }
}
