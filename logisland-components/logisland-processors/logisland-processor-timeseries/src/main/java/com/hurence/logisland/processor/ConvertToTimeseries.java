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


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.SaxEncodingValidators;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

@Tags({"record", "fields", "timeseries", "chronix", "convert"})
@CapabilityDescription("Converts a given field records into a chronix timeseries record")
@ExtraDetailFile("./details/common-processors/EncodeSAX-Detail.rst")
public class ConvertToTimeseries extends AbstractProcessor {
    //TODO delete use others processor instead

    public static final PropertyDescriptor GROUPBY = new PropertyDescriptor.Builder()
            .name("groupby")
            .description("The field the chunk should be grouped by")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .defaultValue("")
            .build();

    //TODO add possibility to choose a ddcThreshold (needs to add possibility in code before)

    public static final PropertyDescriptor AGGS = new PropertyDescriptor.Builder()
            .name("aggs")
            .description("The agregations to calculates for the chunk")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)//TODO use validator with enum
//            .defaultValue("")//TODO all ? include ALL and NONE in enum
            .build();

    public static final PropertyDescriptor SAX_ENCODING = new PropertyDescriptor.Builder()
            .name("sax.encoding")
            .description("whether to add a sax encoding version of the chunk")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor SAX_ENCODING_PAA_SIZE = new PropertyDescriptor.Builder()
            .name("sax.encoding.paa.size")
            .description("the size of resulting sax string")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("3")
            .build();

    public static final PropertyDescriptor SAX_ENCODING_N_THRESHOLD = new PropertyDescriptor.Builder()
            .name("sax.encoding.threshold")
            .description("Used to normalize values before encoding into sax string")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_DOUBLE_VALIDATOR)
            .defaultValue("0")
            .build();

    public static final PropertyDescriptor SAX_ENCODING_ALPHABET_SIZE = new PropertyDescriptor.Builder()
            .name("sax.encoding.alphabet.size")
            .description("The number of different letter of the alphabet to use")
            .required(false)
            .addValidator(SaxEncodingValidators.ALPHABET_SIZE_VALIDATOR)
            .defaultValue("3")
            .build();

    public static final PropertyDescriptor BINARY_COMPACTION = new PropertyDescriptor.Builder()
            .name("sax.encoding")
            .description("whether to compact time series into binary format")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor BINARY_COMPACTION_THRESHOLD = new PropertyDescriptor.Builder()
            .name("sax.encoding.threshold")
            .description("Used to normalize values before encoding into binaries format")//TODO check in detail
            .required(false)
            .addValidator(StandardValidators.POSITIVE_DOUBLE_VALIDATOR)
            .defaultValue("0")
            .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(GROUPBY);
        descriptors.add(AGGS);
        descriptors.add(SAX_ENCODING);
        return descriptors;
    }

    private BinaryCompactionConverter converter;
    private List<String> groupBy;

    @Override
    public void init(final ProcessContext context) {
        super.init(context);
        final String[] groupByArray = context.getPropertyValue(GROUPBY).asString().split(",");
        groupBy = Arrays.stream(groupByArray)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        BinaryCompactionConverter.Builder builder = new BinaryCompactionConverter.Builder();
        boolean saxEncoding = context.getPropertyValue(SAX_ENCODING).asBoolean();
//        builder.saxEncoding(saxEncoding);
//        if (saxEncoding) {
//            final int paaSize = context.getPropertyValue(SAX_ENCODING_PAA_SIZE).asInteger();
//            final double threshold = context.getPropertyValue(SAX_ENCODING_N_THRESHOLD).asDouble();
//            final int alphabetSize = context.getPropertyValue(SAX_ENCODING_ALPHABET_SIZE).asInteger();
//            builder.alphabetSize(alphabetSize)
//                    .nThreshold(threshold)
//                    .paaSize(paaSize);
//        }
//        boolean binaryCompaction = context.getPropertyValue(BINARY_COMPACTION).asBoolean();
//        builder.binaryCompaction(binaryCompaction);
//        if (binaryCompaction) {
//            final int threshold = context.getPropertyValue(BINARY_COMPACTION_THRESHOLD).asInteger();
//            builder.ddcThreshold(threshold);
//        }
        converter = builder.build();
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
                    .map(converter::chunk)
                    .collect(Collectors.toList());
        }

        return outputRecords;
    }

}

