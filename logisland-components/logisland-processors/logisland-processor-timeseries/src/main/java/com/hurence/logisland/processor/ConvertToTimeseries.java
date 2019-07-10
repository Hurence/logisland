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
import com.hurence.logisland.timeseries.functions.*;
import com.hurence.logisland.timeseries.metric.MetricType;
import com.hurence.logisland.timeseries.query.QueryEvaluator;
import com.hurence.logisland.timeseries.query.TypeFunctions;
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


    public static final PropertyDescriptor METRIC = new PropertyDescriptor.Builder()
            .name("metric")
            .description("The chronix metric to calculate for the chunk")
            .required(false)
            .addValidator(StandardValidators.SEMICOLON_SEPARATED_LIST_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(GROUPBY);
        descriptors.add(METRIC);
        return descriptors;
    }

    private List<ChronixTransformation> transformations;
    private List<ChronixAggregation> aggregations;
    private List<ChronixAnalysis> analyses;
    private List<ChronixEncoding> encodings;
    private FunctionValueMap functionValueMap;

    private BinaryCompactionConverter converter;
    private List<String> groupBy;

    @Override
    public void init(final ProcessContext context) {
        super.init(context);

        // init binary converter
        final String[] groupByArray = context.getPropertyValue(GROUPBY).asString().split(",");
        groupBy = Arrays.stream(groupByArray)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        BinaryCompactionConverter.Builder builder = new BinaryCompactionConverter.Builder();
        converter = builder.build();

        // init metric functions
        if (context.getPropertyValue(METRIC).isSet()) {
            String[] metric = {context.getPropertyValue(METRIC).asString()};

            TypeFunctions functions = QueryEvaluator.extractFunctions(metric);

            analyses = functions.getTypeFunctions(new MetricType()).getAnalyses();
            aggregations = functions.getTypeFunctions(new MetricType()).getAggregations();
            transformations = functions.getTypeFunctions(new MetricType()).getTransformations();
            encodings = functions.getTypeFunctions(new MetricType()).getEncodings();
            functionValueMap = new FunctionValueMap(aggregations.size(), analyses.size(), transformations.size(), encodings.size());
        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        List<Record> outputRecords = Collections.emptyList();

        Map<String, List<Record>> groups = records.stream().collect(
                Collectors.groupingBy(r -> groupBy.stream()
                        .map(f -> r.hasField(f) ? r.getField(f).asString() : null)
                        .collect(Collectors.joining("|"))
                )
        );

        if (!groups.isEmpty()) {
            outputRecords = groups.values().stream()
                    .filter(l -> !l.isEmpty())                                      // remove empty groups
                    .peek(recs -> recs.sort(Comparator.comparing(Record::getTime))) // sort by time asc
                    //     transformations.forEach(transformation -> transformation.execute(ts, functionValueMap));
                    .map(converter::chunk)
                    .collect(Collectors.toList());
        }

        return outputRecords;
    }

}

