package com.hurence.logisland.timeseries.converter;

import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.*;
import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.dts.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class RecordsTimeSeriesConverter implements Serializable {


    private Logger logger = LoggerFactory.getLogger(RecordsTimeSeriesConverter.class.getName());
    MetricTimeSeriesConverter converter = new MetricTimeSeriesConverter();

    /**
     * Compact a related list of records a single chunked one
     *
     * @param records
     * @return
     * @throws ProcessException
     */
    public Record chunk(List<Record> records) throws ProcessException {

        Record first = records.get(0);
        String batchUID = UUID.randomUUID().toString();
        final long firstTS = records.get(0).getTime().getTime();
        long tmp = records.get(records.size() - 1).getTime().getTime();
        final long lastTS = tmp == firstTS ? firstTS + 1 : tmp;


        //extract meta
        String metricType = records.stream().filter(record -> record.hasField(FieldDictionary.RECORD_TYPE) &&
                record.getField(FieldDictionary.RECORD_TYPE).getRawValue() != null)
                .map(record -> record.getField(FieldDictionary.RECORD_TYPE).asString())
                .findFirst().orElse(RecordDictionary.METRIC);

        String metricName = records.stream().filter(record -> record.hasField(FieldDictionary.RECORD_NAME) &&
                record.getField(FieldDictionary.RECORD_NAME).getRawValue() != null)
                .map(record -> record.getField(FieldDictionary.RECORD_NAME).asString())
                .findFirst().orElse("unknown");

        Map<String, Object> attributes = first.getAllFieldsSorted().stream()
                .filter(field -> !field.getName().equals(FieldDictionary.RECORD_TIME) &&
                        !field.getName().equals(FieldDictionary.RECORD_NAME) &&
                        !field.getName().equals(FieldDictionary.RECORD_VALUE) &&
                        !field.getName().equals(FieldDictionary.RECORD_ID) &&
                        !field.getName().equals(FieldDictionary.RECORD_TYPE) &&
                        !field.getName().equals(FieldDictionary.RECORD_START) &&
                        !field.getName().equals(FieldDictionary.RECORD_END))
                .collect(Collectors.toMap(field -> field.getName().replaceAll("\\.", "_"),
                        field -> {
                            try {
                                switch (field.getType()) {
                                    case STRING:
                                        return field.asString();
                                    case INT:
                                        return field.asInteger();
                                    case LONG:
                                        return field.asLong();
                                    case FLOAT:
                                        return field.asFloat();
                                    case DOUBLE:
                                        return field.asDouble();
                                    case BOOLEAN:
                                        return field.asBoolean();
                                    default:
                                        return field.getRawValue();
                                }
                            } catch (Exception e) {
                                logger.error("Unable to process field " + field, e);
                                return null;
                            }
                        }
                ));

        MetricTimeSeries.Builder ret = new MetricTimeSeries.Builder(metricName, metricType)
                .attributes(attributes)
                .attribute("id", batchUID)
                .start(firstTS)
                .end(lastTS);

        records.stream()
                .filter(record -> record.getField(FieldDictionary.RECORD_VALUE) != null && record.getField(FieldDictionary.RECORD_VALUE).getRawValue() != null)
                .map(record -> new Pair<>(record.getTime().getTime(), record.getField(FieldDictionary.RECORD_VALUE).asDouble()))
                .filter(longDoublePair -> longDoublePair.getSecond() != null && Double.isFinite(longDoublePair.getSecond()))
                .forEach(pair -> ret.point(pair.getFirst(), pair.getSecond()));

        MetricTimeSeries metricTimeSeries = ret.build();
        BinaryTimeSeries binaryTimeSeries = converter.to(metricTimeSeries);


        Record chunkrecord = new StandardRecord(RecordDictionary.METRIC)
                .setField(FieldDictionary.RECORD_START, FieldType.LONG, binaryTimeSeries.getStart())
                .setField(FieldDictionary.RECORD_END, FieldType.LONG, binaryTimeSeries.getEnd())
                .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, binaryTimeSeries.getPoints())
                .setField(FieldDictionary.RECORD_NAME, FieldType.STRING, binaryTimeSeries.getName());

        attributes.keySet().forEach(key -> {
            chunkrecord.setField(key, FieldType.STRING, binaryTimeSeries.getFields().get(key));
        });

        return chunkrecord;

    }


    /**
     * Reverse operation for chunk operation
     *
     * @param record
     * @return
     * @throws ProcessException
     */
    public List<Record> unchunk(Record record) throws ProcessException {

        String name = record.getField(FieldDictionary.RECORD_NAME).asString();
        long start = record.getField(FieldDictionary.RECORD_START).asLong();
        long end = record.getField(FieldDictionary.RECORD_END).asLong();
        String type = record.getField(FieldDictionary.RECORD_TYPE).asString();

        BinaryTimeSeries.Builder ret = new BinaryTimeSeries.Builder()
                .start(start)
                .end(end)
                .name(name)
                .data(record.getField(FieldDictionary.RECORD_VALUE).asBytes());


        MetricTimeSeries metricTimeSeries = converter.from(ret.build(), start, end);

        return metricTimeSeries.points().map(m -> {
            long recordTime = m.getTimestamp();
            double recordValue = m.getValue();

            return new StandardRecord(type)
                    .setField(FieldDictionary.RECORD_NAME, FieldType.STRING, name)
                    .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, recordTime)
                    .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, recordValue);
        }).collect(Collectors.toList());
    }

}
