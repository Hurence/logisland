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
package com.hurence.logisland.processor.alerting;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.*;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.*;

@Tags({"record", "threshold", "tag", "alerting"})
@CapabilityDescription("Compute threshold cross from given formulas.\n\n" +
        "- each dynamic property will return a new record according to the formula definition\n" +
        "- the record name will be set to the property name\n" +
        "- the record time will be set to the current timestamp\n")
@ExtraDetailFile("./details/common-processors/CheckThresholds-Detail.rst")
@DynamicProperty(name = "field to add",
        supportsExpressionLanguage = false,
        value = "a default value",
        description = "Add a field to the record with the default value")
public class CheckThresholds extends AbstractNashornSandboxProcessor {


    /*
            - processor: compute_thresholds
          component: com.hurence.logisland.processor.CheckThresholdCross
          type: processor
          documentation: |
            compute threshold cross from given formulas.
            each dynamic property will return a new record according to the formula definition
            the record name will be set to the property name
            the record time will be set to the current timestamp

            a threshold_cross has the following properties : count, sum, avg, time, duration, value
          configuration:
            cache.client.service: cache
            default.record_type: threshold_cross
            default.ttl: 300000
            tvib1: cache("vib1").value > 10.0;
            tvib2: cache("vib2").value >= 0 && cache("vib2").value < cache("vib1").value;
     */

    private static final Logger logger = LoggerFactory.getLogger(CheckThresholds.class);


    public static final PropertyDescriptor RECORD_TTL = new PropertyDescriptor.Builder()
            .name("record.ttl")
            .description("How long (in ms) do the record will remain in cache")
            .required(false)
            .defaultValue("30000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();


    public static final PropertyDescriptor MIN_UPDATE_TIME_MS = new PropertyDescriptor.Builder()
            .name("min.update.time.ms")
            .description("The minimum amount of time (in ms) that we expect between two consecutive update of the same threshold record")
            .required(false)
            .defaultValue("200")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(RECORD_TTL);
        properties.add(MIN_UPDATE_TIME_MS);

        return properties;
    }

    @Override
    protected void setupDynamicProperties(ProcessContext context) {
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            String key = entry.getKey().getName();
            String value = entry.getValue()
                    .replaceAll("cache\\((\\S*\\))", "cache.get(\"test\", new com.hurence.logisland.record.StandardRecord().setId($1)")
                    .replaceAll("\\.value", ".getField(com.hurence.logisland.record.FieldDictionary.RECORD_VALUE).asDouble()");

            StringBuilder sb = new StringBuilder();
            sb.append("var match=false;\n")
                    .append("try {\n")
                    .append("if( ")
                    .append(value)
                    .append(" ) { match=true; }\n")
                    .append("} catch(error) {}");

            dynamicTagValuesMap.put(entry.getKey().getName(), sb.toString());
            //   System.out.println(sb.toString());
            //  logger.debug(sb.toString());
        }
        defaultCollection = context.getPropertyValue(DATASTORE_CACHE_COLLECTION).asString();
        recordTTL = context.getPropertyValue(RECORD_TTL).asInteger();
    }

    private String defaultCollection;
    private Integer recordTTL;

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {


        List<Record> outputRecords = new ArrayList<>(records);
        for (final Map.Entry<String, String> entry : dynamicTagValuesMap.entrySet()) {

            // look for record into the cache & remove this if TTL expired
            String key = entry.getKey();
            Record cachedThreshold = datastoreClientService.get(defaultCollection, new StandardRecord().setId(key));
            if (cachedThreshold != null) {
                Long duration = System.currentTimeMillis() - cachedThreshold.getTime().getTime();
                if (duration > recordTTL) {
                    datastoreClientService.remove(defaultCollection, cachedThreshold, false);
                    cachedThreshold = null;
                }
            }

            try {
                sandbox.eval(entry.getValue());
                Boolean match = (Boolean) sandbox.get("match");
                if (match) {
                    if (cachedThreshold != null) {
                        // check if we haven't handle this event yet
                        Long durationBeetwenLastUpdateInMs = System.currentTimeMillis() -
                                cachedThreshold.getField(FieldDictionary.RECORD_LAST_UPDATE_TIME).asLong();
                        if (durationBeetwenLastUpdateInMs > context.getPropertyValue(MIN_UPDATE_TIME_MS).asLong()) {

                        Long count = cachedThreshold.getField(FieldDictionary.RECORD_COUNT).asLong();
                        Date firstThresholdTime = cachedThreshold.getTime();
                        cachedThreshold.setField(FieldDictionary.RECORD_COUNT, FieldType.LONG, count + 1)
                                .setTime(firstThresholdTime)
                                .setField(FieldDictionary.RECORD_LAST_UPDATE_TIME, FieldType.LONG, System.currentTimeMillis());

                        datastoreClientService.put(defaultCollection, cachedThreshold, true);
                        outputRecords.add(cachedThreshold);
                        }

                    } else {
                        Record threshold = new StandardRecord(outputRecordType)
                                .setId(key)
                                .setStringField(FieldDictionary.RECORD_VALUE, context.getPropertyValue(key).asString())
                                .setField(FieldDictionary.RECORD_COUNT, FieldType.LONG, 1L)
                                .setField(FieldDictionary.RECORD_LAST_UPDATE_TIME, FieldType.LONG, System.currentTimeMillis());
                        datastoreClientService.put(defaultCollection, threshold, true);
                        outputRecords.add(threshold);
                    }
                }
            } catch (ScriptException e) {
                Record errorRecord = new StandardRecord(RecordDictionary.ERROR)
                        .setId(entry.getKey())
                        .addError("ScriptException", e.getMessage());
                //   outputRecords.add(errorRecord);
                logger.error(e.toString());
            }
        }

        return outputRecords;
    }
}