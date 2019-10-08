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
import com.hurence.logisland.component.SaxEncodingValidators;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.TimeSeriesRecord;
import com.hurence.logisland.timeseries.MetricTimeSeries;
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

    private List<ChronixTransformation> transformations = Collections.emptyList();
    private List<ChronixAggregation> aggregations = Collections.emptyList();
    private List<ChronixAnalysis> analyses = Collections.emptyList();
    private List<ChronixEncoding> encodings = Collections.emptyList();
    private FunctionValueMap functionValueMap = new FunctionValueMap(0, 0, 0, 0);

    private BinaryCompactionConverter converter;
    private List<String> groupBy;

    @Override
    public void init(final ProcessContext context) throws InitializationException{
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
            String[] metric = {"metric{" + context.getPropertyValue(METRIC).asString() + "}"};

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
                    .map(groupedRecords -> {
                        TimeSeriesRecord tsRecord = converter.chunk(groupedRecords);
                        MetricTimeSeries timeSeries = tsRecord.getTimeSeries();

                        transformations.forEach(transfo -> transfo.execute(timeSeries, functionValueMap));
                        analyses.forEach(analyse -> analyse.execute(timeSeries, functionValueMap));
                        aggregations.forEach(aggregation -> aggregation.execute(timeSeries, functionValueMap));
                        encodings.forEach(encoding -> encoding.execute(timeSeries, functionValueMap));

                        for (int i = 0; i < functionValueMap.sizeOfAggregations(); i++) {
                            String name = functionValueMap.getAggregation(i).getQueryName();
                            double value = functionValueMap.getAggregationValue(i);
                            tsRecord.setField("chunk_" + name, FieldType.DOUBLE, value);
                        }

                        for (int i = 0; i < functionValueMap.sizeOfAnalyses(); i++) {
                            String name = functionValueMap.getAnalysis(i).getQueryName();
                            boolean value = functionValueMap.getAnalysisValue(i);
                            tsRecord.setField("chunk_" + name, FieldType.BOOLEAN, value);
                        }

                        for (int i = 0; i < functionValueMap.sizeOfEncodings(); i++) {
                            String name = functionValueMap.getEncoding(i).getQueryName();
                            String value = functionValueMap.getEncodingValue(i);
                            tsRecord.setField("chunk_" + name, FieldType.STRING, value);
                        }

                        return tsRecord;
                    })
                    .collect(Collectors.toList());
        }

        return outputRecords;
    }

}

