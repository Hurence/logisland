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
package com.hurence.logisland.processor;

/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hurence.logisland.component.InitializationException;

    import com.hurence.logisland.annotation.documentation.CapabilityDescription;
    import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
    import com.hurence.logisland.annotation.documentation.Tags;
    import com.hurence.logisland.component.PropertyDescriptor;
    import com.hurence.logisland.record.FieldDictionary;
    import com.hurence.logisland.record.Point;
    import com.hurence.logisland.record.Record;
    import com.hurence.logisland.record.StandardRecord;
    import com.hurence.logisland.timeseries.converter.common.Compression;
    import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
    import com.hurence.logisland.timeseries.dts.Pair;
    import com.hurence.logisland.validator.StandardValidators;
    import org.apache.commons.lang3.StringUtils;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.util.*;
    import java.util.stream.Collectors;
    import java.util.stream.Stream;

@Tags({"record", "fields", "timeseries", "chronix", "convert"})
@CapabilityDescription("Converts a given field records into a chronix timeseries record")
@ExtraDetailFile("./details/common-processors/EncodeSAX-Detail.rst")
public class CompactChunkProcessor extends AbstractProcessor {

    //TODO delete use others processor instead

    private final static Logger logger = LoggerFactory.getLogger(CompactChunkProcessor.class.getName());
    public static final String DYNAMIC_PROPS_PREFIX = "field.";

    public static final PropertyDescriptor GROUP_BY_FIELD = new PropertyDescriptor.Builder()
            .name("group.by.field")
            .description("The field the chunk should be grouped by")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .defaultValue("")
            .build();

    public static final PropertyDescriptor BINARY_COMPACTION_THRESHOLD = new PropertyDescriptor.Builder()
            .name("sax.encoding.threshold")
            .description("Used to normalize values before encoding into binaries format")//TODO check in detail
            .required(false)
            .addValidator(StandardValidators.POSITIVE_DOUBLE_VALIDATOR)
            .defaultValue("0")
            .build();

    private Collection<String> fieldsToCompact;
    private List<String> groupBy;
    private int threshold;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(GROUP_BY_FIELD);
        descriptors.add(BINARY_COMPACTION_THRESHOLD);
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        if (propertyDescriptorName==null || !propertyDescriptorName.startsWith(DYNAMIC_PROPS_PREFIX))
            return null;
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName.substring(DYNAMIC_PROPS_PREFIX.length()))
                .description("field that should contain a double's array to compact")
                .required(false)
                .build();
    }

    @Override
    public void init(final ProcessContext context) throws InitializationException{
        super.init(context);
        final String[] groupByArray = context.getPropertyValue(GROUP_BY_FIELD).asString().split(",");
        groupBy = Arrays.stream(groupByArray)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        threshold = context.getPropertyValue(BINARY_COMPACTION_THRESHOLD).asInteger();
        fieldsToCompact = getFieldsToChunk(context);
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        List<Record> outputRecords = Collections.emptyList();

        Map<String, List<Record>> groups = records.stream().collect(
                Collectors.groupingBy(r ->
                        groupBy
                                .stream().map(f -> r.hasField(f) ? r.getField(f).asString() : null)
                                .collect(Collectors.joining("|"))
                ));

        if (!groups.isEmpty()) {
            outputRecords = groups.values().stream()
                    .filter(l -> !l.isEmpty())
                    .peek(recs -> {
                        recs.sort(Comparator.comparing(Record::getTime));
                    })
                    .map(this::chunkRecords)
                    .collect(Collectors.toList());
        }

        return outputRecords;
    }

    private Record chunkRecords(List<Record> records) {
        final StandardRecord chunkedRecord = new StandardRecord();
        List<Point> points = extractPoints(records.stream()).collect(Collectors.toList());
//        chunkedRecord.setCompressedPoints(compressPoints(points.stream()));
        return null;
    }

    private byte[] compressPoints(Stream<Point> points) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(points.iterator(), threshold);
        return Compression.compress(serializedPoints);
    }

    private Stream<Point> extractPoints(Stream<Record> records) {
        return records
                .filter(record -> record.getField(FieldDictionary.RECORD_VALUE) != null && record.getField(FieldDictionary.RECORD_VALUE).getRawValue() != null)
                .map(record -> new Pair<>(record.getTime().getTime(), record.getField(FieldDictionary.RECORD_VALUE).asDouble()))
                .filter(longDoublePair -> longDoublePair.getSecond() != null && Double.isFinite(longDoublePair.getSecond()))
                .map(pair -> new Point(0 ,pair.getFirst(), pair.getSecond()));
    }
    /*
     * Build a map of mapping rules of the form:
     *
     */
    private Collection<String> getFieldsToChunk(ProcessContext context) {
        /**
         * list alternative regex
         */
        Collection<String> fieldsToChunk = new ArrayList<>();
        // loop over dynamic properties to add alternative regex
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final String fieldName = entry.getKey().getName();
            if (entry.getValue() !=null && !entry.getValue().isEmpty()) {
                logger.warn("The mapping rule for {} has the invalid value: {}", entry.getKey().getName(), entry.getValue());
            }
        }
        return fieldsToChunk;
    }
}

