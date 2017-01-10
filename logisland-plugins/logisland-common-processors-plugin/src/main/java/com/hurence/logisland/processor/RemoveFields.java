package com.hurence.logisland.processor;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tags({"record", "fields", "remove", "delete"})
@CapabilityDescription("Removes a list of fields defined by a comma separated list of field names")
public class RemoveFields extends AbstractProcessor {

    private static final long serialVersionUID = -270933070438408174L;

    private static final Logger logger = LoggerFactory.getLogger(RemoveFields.class);

    public static final PropertyDescriptor FIELDS_TO_REMOVE = new PropertyDescriptor.Builder()
            .name("fields.to.remove")
            .description("the comma separated list of field names (e.g. \"policyid,date_raw\"")
            .required(true)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        try{
            List<String> fieldsToRemove = Lists.newArrayList(
                    context.getPropertyValue(FIELDS_TO_REMOVE).asString().split(","));

            for (Record record : records) {
                new ArrayList<>(record.getAllFields()).forEach(field -> {
                    String fieldName = field.getName();
                    if (fieldsToRemove.contains(fieldName)) {
                        record.removeField(fieldName);
                    }
                });
            }
        }catch (Exception ex){
            logger.warn("issue while trying to remove field list {} :  {}",
                    context.getPropertyValue(FIELDS_TO_REMOVE).asString(),
                    ex.toString());
        }

        return records;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(
                Lists.newArrayList(FIELDS_TO_REMOVE));
    }
}
