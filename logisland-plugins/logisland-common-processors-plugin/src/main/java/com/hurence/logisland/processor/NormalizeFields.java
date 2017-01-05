package com.hurence.logisland.processor;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NormalizeFields extends AbstractProcessor {

    private static final long serialVersionUID = -270933070438408174L;

    private static final Logger logger = LoggerFactory.getLogger(NormalizeFields.class);

    public static final PropertyDescriptor FIELDS_NAME_MAPPING = new PropertyDescriptor.Builder()
            .name("fields_name.mapping")
            .description("the mapping to convert names (e.g. \"policy_id\" --> \"policyid\"")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> SUPPORTED_PROPERTIES = Collections.unmodifiableList(
            Lists.newArrayList(FIELDS_NAME_MAPPING));


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        Map<String, String> fiendsNameMapping = getFieldsNameMapping(context);
        for (Record record : records) {
            normalizeRecord(record, fiendsNameMapping);
        }
        return records;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        Map<String, String> fieldsNameMapping = getFieldsNameMapping(context.getPropertyValue(FIELDS_NAME_MAPPING).asString());

        Set<String> keys = fieldsNameMapping.keySet();
        Collection<String> values = fieldsNameMapping.values();

        keys.forEach(k -> {
            if (values.contains(k)) {
                validationResults.add(
                        new ValidationResult.Builder()
                                .input(FIELDS_NAME_MAPPING.getName())
                                .explanation(String.format("key %s maps is mapped by another entry. Mapping chains or circles are not allowed (e.g. s1 -> s2, s2 -> s3)", k))
                                .valid(false)
                                .build());
            }
        });

        return validationResults;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return SUPPORTED_PROPERTIES;
    }




    private void normalizeRecord(Record record, Map<String, String> fieldsNameMapping) {
        new ArrayList<>(record.getAllFields())
                .forEach(f -> {
                    String currentName = f.getName();
                    String newName = fieldsNameMapping.get(currentName);
                    if (newName != null) {
                        // Note that we will systematically overwrite any field named newName
                        record.removeField(currentName);
                        record.setField(newName, f.getType(), f.getRawValue());
                    }
                });
    }

    private Map<String, String> getFieldsNameMapping(ProcessContext context) {
        PropertyValue fieldsNameMappingJson = context.getPropertyValue(FIELDS_NAME_MAPPING);
        return getFieldsNameMapping(fieldsNameMappingJson.getRawValue().toString());
    }

    private Map<String, String> getFieldsNameMapping(String fieldsNameMappingJson) {
        try {
            return new Gson().fromJson(
                    fieldsNameMappingJson,
                    new TypeToken<Map<String, String>>() {
                    }.getType());
        } catch (JsonSyntaxException e) {
            throw new ProcessException("The names mapping field syntax is incorect: " + e.getMessage());
        }
    }
}
