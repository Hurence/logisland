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
package com.hurence.logisland.processor.alerting;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordDictionary;
import com.hurence.logisland.record.StandardRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Tags({"record", "fields", "Add"})
@CapabilityDescription("Compute tag cross from given formulas.\n" +
        "- each dynamic property will return a new record according to the formula definition\n" +
        "- the record name will be set to the property name\n" +
        "- the record time will be set to the current timestamp\n\n" +
        "a threshold_cross has the following properties : count, sum, avg, time, duration, value")
@DynamicProperty(name = "field to add",
        supportsExpressionLanguage = false,
        value = "a default value",
        description = "Add a field to the record with the default value")
public class ComputeTags extends AbstractNashornSandboxProcessor {


    private static final Logger logger = LoggerFactory.getLogger(ComputeTags.class);


    @Override
    protected void setupDynamicProperties(ProcessContext context) {

        StringBuilder sbActivation = new StringBuilder();
        sbActivation
                .append("function getValue(id) {\n")
                .append("  var record = cache.get(\"test\", new com.hurence.logisland.record.StandardRecord().setId(id));\n")
                .append("  if(record === undefined) return Double.NaN;\n")
                .append("  else return record.getField(com.hurence.logisland.record.FieldDictionary.RECORD_VALUE).asDouble(); \n};\n");


        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            String key = entry.getKey().getName();
            String value = entry.getValue().replaceAll("cache\\((\\S*)\\).value", "getValue($1)");

            StringBuilder sb = new StringBuilder(sbActivation);
            sb.append("function ")
                    .append(key)
                    .append("() { ")
                    .append(value)
                    .append(" }; \n");
            sb.append("try {\n");
            sb.append("var record_")
                    .append(key)
                    .append(" = new com.hurence.logisland.record.StandardRecord(\"")
                    .append(outputRecordType)
                    .append("\")")
                    .append(".setId(\"")
                    .append(key)
                    .append("\");\n");
            sb.append("record_")
                    .append(key)
                    .append(".setField( ")
                    .append("\"record_value\",")
                    .append(" com.hurence.logisland.record.FieldType.DOUBLE,")
                    .append(key)
                    .append("());\n");
            sb.append("}\ncatch(error){}");

            dynamicTagValuesMap.put(entry.getKey().getName(), sb.toString());
            //     System.out.println(sb.toString());
            //    logger.debug(sb.toString());
        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        // check if we need initialization
        if (datastoreClientService == null) {
            init(context);
        }

        List<Record> outputRecords = new ArrayList<>(records);
        for (final Map.Entry<String, String> entry : dynamicTagValuesMap.entrySet()) {


            try {
                sandbox.eval(entry.getValue());
                Record cached = (Record) sandbox.get("record_" + entry.getKey());
                if (cached.hasField(FieldDictionary.RECORD_VALUE))
                    outputRecords.add(cached);
            } catch (ScriptException e) {
                Record errorRecord = new StandardRecord(RecordDictionary.ERROR)
                        .setId(entry.getKey())
                        .addError("ScriptException", e.getMessage());
                //  outputRecords.add(errorRecord);
                logger.error(e.toString());
            }
        }

        return outputRecords;
    }
}