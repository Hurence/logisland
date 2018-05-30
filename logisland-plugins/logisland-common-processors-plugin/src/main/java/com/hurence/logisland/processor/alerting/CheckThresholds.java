/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.*;

@Tags({"record", "threshold", "tag", "alerting"})
@CapabilityDescription("Compute threshold cross from given formulas.\n" +
        "            each dynamic property will return a new record according to the formula definition\n" +
        "            the record name will be set to the property name\n" +
        "            the record time will be set to the current timestamp")
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
            default.el.language: js
            default.ttl: 300000
            tvib1: cache("vib1").value > 10.0;
            tvib2: cache("vib2").value >= 0 && cache("vib2").value < cache("vib1").value;
     */

    private static final Logger logger = LoggerFactory.getLogger(CheckThresholds.class);

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
            sb.append("var match=false;\n");
            sb.append("if( ")
                    .append(value)
                    .append(" ) { match=true; }\n");

            dynamicTagValuesMap.put(entry.getKey().getName(), sb.toString());
            System.out.println(sb.toString());
            logger.debug(sb.toString());
        }
    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        // check if we need initialization
        if (datastoreClientService == null) {
            init(context);
        }

        List<Record> outputRecords = new ArrayList<>();
        for (final Map.Entry<String, String> entry : dynamicTagValuesMap.entrySet()) {


            try {
                sandbox.eval(entry.getValue());
                Boolean match = (Boolean) sandbox.get("match");
                if (match) {

                    String key = entry.getKey();
                    Record cachedThreshold = datastoreClientService.get("test", new StandardRecord().setId(key));
                    if (cachedThreshold != null) {

                        Long count = cachedThreshold.getField("count").asLong();
                        Long duration = System.currentTimeMillis() - cachedThreshold.getField("first_record_time").asLong();
                        cachedThreshold.setStringField(FieldDictionary.RECORD_VALUE, context.getPropertyValue(key).asString())
                                .setField("count", FieldType.LONG, count + 1)
                                .setField("duration", FieldType.LONG, duration);
                        outputRecords.add(cachedThreshold);
                    } else {
                        Record threshold = new StandardRecord(RecordDictionary.THRESHOLD)
                                .setId(key)
                                .setStringField(FieldDictionary.RECORD_VALUE, context.getPropertyValue(key).asString())
                                .setField("count", FieldType.LONG, 1L)
                                .setField("first_record_time", FieldType.LONG, new Date().getTime())
                                .setField("duration", FieldType.LONG, 0);
                        datastoreClientService.put("test", threshold, true);
                        outputRecords.add(threshold);
                    }
                }
            } catch (ScriptException e) {
                Record errorRecord = new StandardRecord(RecordDictionary.ERROR)
                        .setId(entry.getKey())
                        .addError("ScriptException", e.getMessage());
                outputRecords.add(errorRecord);
                logger.error(e.toString());
            }
        }

        return outputRecords;
    }
}