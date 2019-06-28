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
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordDictionary;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.*;

@Tags({"record", "alerting", "thresholds", "opc", "tag"})
@CapabilityDescription("Add one or more records representing alerts. Using a datastore.")
@ExtraDetailFile("./details/common-processors/CheckAlerts-Detail.rst")
@DynamicProperty(name = "field to add",
        supportsExpressionLanguage = false,
        value = "a default value",
        description = "Add a field to the record with the default value")
public class CheckAlerts extends AbstractNashornSandboxProcessor {


    public static final PropertyDescriptor PROFILE_ACTIVATION_CONDITION = new PropertyDescriptor.Builder()
            .name("profile.activation.condition")
            .description("A javascript expression that activates this alerting profile when true")
            .required(false)
            .defaultValue("0==0")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ALERT_CRITICITY = new PropertyDescriptor.Builder()
            .name("alert.criticity")
            .description("from 0 to ...")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PROFILE_ACTIVATION_CONDITION);
        properties.add(ALERT_CRITICITY);

        return properties;
    }



    /*
        - processor: compute_alerts1
          component: com.hurence.logisland.processor.CheckAlertOnThresholds
          type: processor
          documentation: |
            compute threshold cross from given formulas.
            each dynamic property will return a new record according to the formula definition
            the record name will be set to the property name
            the record time will be set to the current timestamp
          configuration:
            cache.client.service: cache
            default.record_type: alert
            default.el.language: js
            default.criticity.level: 1
            profile.activation.condition: cache("cvib1").value < 10.0 && cache("vib2").value > 2;
            avib1: cache("tvib1").count > 5.0 * cache("cvib1");
            avib2: cache("tvib2").avg > 12.0;
            avib3: cache("tvib2").duration > 12000.0;
     */

    private static final Logger logger = LoggerFactory.getLogger(CheckAlerts.class);


    private String expandCode(String rawCode) {
        return rawCode.replaceAll("cache\\((\\S*)\\).value", "getValue($1)")
                .replaceAll("cache\\((\\S*)\\).count", "getCount($1)")
                .replaceAll("cache\\((\\S*)\\).duration", "getDuration($1)");
    }


    @Override
    protected void setupDynamicProperties(ProcessContext context) {

        sandbox.allow(System.class);
        sandbox.allow(Date.class);
        sandbox.allow(Double.class);
        String profileActivationRule = context.getPropertyValue(PROFILE_ACTIVATION_CONDITION).asString();

        StringBuilder sbActivation = new StringBuilder();
        sbActivation.append("var alert = false;\n")
                .append("function getValue(id) {\n")
                .append("  var record = cache.get(\"test\", new com.hurence.logisland.record.StandardRecord().setId(id));\n")
                .append("  if(record === null) return Double.NaN;\n")
                .append("  else return record.getField(com.hurence.logisland.record.FieldDictionary.RECORD_VALUE).asDouble(); \n};\n")
                .append("function getDuration(id) {\n")
                .append("  var record = cache.get(\"test\", new com.hurence.logisland.record.StandardRecord().setId(id));\n")
                .append("  if(record === null) return -1;\n")
                .append("  else { \n")
                .append("    var duration =  new Date().getTime() - record.getTime().getTime();\n")
                .append("    return duration; \n}};\n")
                .append("function getCount(id) {\n")
                .append("  var record = cache.get(\"test\", new com.hurence.logisland.record.StandardRecord().setId(id));\n")
                .append("  if(record === null) return -1;\n")
                .append("  else return record.getField(\"record_count\").asLong(); \n};\n")
                .append("try {\n")
                .append("if( ")
                .append(expandCode(profileActivationRule))
                .append(" ) { \n");


        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            String key = entry.getKey().getName();
            String value = expandCode(entry.getValue());
            StringBuilder sb = new StringBuilder(sbActivation);
            sb.append("  if( ")
                    .append(value)
                    .append(" ) { alert = true; }\n")
                    .append("}\n")
                    .append("} catch(error) {}");

            dynamicTagValuesMap.put(entry.getKey().getName(), sb.toString());

            //  System.out.println(sb.toString());
            // logger.debug(sb.toString());
        }




    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {



        List<Record> outputRecords = new ArrayList<>(records);
        for (final Map.Entry<String, String> entry : dynamicTagValuesMap.entrySet()) {
            try {
                sandbox.eval(entry.getValue());
                Boolean alert = (Boolean) sandbox.get("alert");
                if (alert) {
                    Record alertRecord = new StandardRecord(outputRecordType)
                            .setId(entry.getKey())
                            .setStringField(FieldDictionary.RECORD_VALUE, context.getPropertyValue(entry.getKey()).asString());
                    outputRecords.add(alertRecord);

                    logger.info(alertRecord.toString());
                }
            } catch (ScriptException e) {
                Record errorRecord = new StandardRecord(RecordDictionary.ERROR)
                        .setId(entry.getKey())
                        .addError("ScriptException", e.getMessage());
                // outputRecords.add(errorRecord);
                logger.error(e.toString());
            }
        }

        return outputRecords;
    }
}