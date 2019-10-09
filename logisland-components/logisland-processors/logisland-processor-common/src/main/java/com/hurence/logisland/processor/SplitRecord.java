package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;



import java.util.*;

@CapabilityDescription("This processor is used to create a new set of records from one record.")
@DynamicProperty(name = "new record name",
        supportsExpressionLanguage = true,
        value = "fields to have",
        description = "the new record")
public class SplitRecord extends AbstractProcessor {
    static final long serialVersionUID = 1413578915552852739L;

    public static final PropertyDescriptor KEEP_PARENT_RECORD = new PropertyDescriptor.Builder()
            .name("keep.parent.record")
            .description("Specify if the parent record should exist")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor KEEP_PARENT_RECORD_TYPE = new PropertyDescriptor.Builder()
            .name("keep.parent.record_type")
            .description("Specify whether to use the dynamic property name as record_type or not")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor KEEP_PARENT_RECORD_TIME = new PropertyDescriptor.Builder()
            .name("keep.parent.record_time")
            .description("Specify whether to use the processing_time as record_time or not")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(KEEP_PARENT_RECORD);
        descriptors.add(KEEP_PARENT_RECORD_TIME);
        descriptors.add(KEEP_PARENT_RECORD_TYPE);
        return Collections.unmodifiableList(descriptors);
    }
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(true)
                .dynamic(true)
                .build();
    }
    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);
    }
    /*@Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        try {

        } catch (Exception ex) {
            validationResults.add(
                    new ValidationResult.Builder()
                            .input(ex.getMessage())
                            .valid(false)
                            .build());
        }
        return validationResults;
    }*/

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        final boolean keepParent = context.getPropertyValue(KEEP_PARENT_RECORD).asBoolean();
        final boolean keepParentType = context.getPropertyValue(KEEP_PARENT_RECORD_TYPE).asBoolean();
        final boolean keepParentTime = context.getPropertyValue(KEEP_PARENT_RECORD_TIME).asBoolean();
        Collection<Record> oringinRecords = new ArrayList<>();
        try {
            init(context);
        } catch (Throwable t) {
            getLogger().error("error while initializing", t);
        }
        try {
            for (Record record : records) {
                for (Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                    if (!entry.getKey().isDynamic()) {
                        continue;
                    }
                    String newRecordType = entry.getKey().getName();
                    String fieldName = entry.getValue();
                    List<String> fieldNames = getFieldsNames(fieldName);
                    Map<String, Field> map = new HashMap<>();
                    Set<Map.Entry<String, Field>> fields = record.getFieldsEntrySet();
                    boolean errorField = false;
                    String errorList = "";
                    for (String field : fieldNames) {
                        if (record.hasField(field))
                            map.put(field, record.getField(field));
                        else {
                            errorField = true;
                            errorList = errorList.concat(String.format("there is no field %s \n",field));
                        }
                    }
                    Field idField = new Field(record.getId());
                    map.put("parent_record_id", idField);
                    long recordTime = 0;
                    if(!keepParentTime) {
                        recordTime = System.nanoTime();
                    } else {recordTime = record.getTime().getTime(); }
                    String recordType = null;
                    if (keepParentType) {
                        recordType = record.getType();
                    } else {recordType = newRecordType;}
                    if (errorField) {
                        oringinRecords.add(new StandardRecord().setFields(map).addError("there are some field(s) that don't exist : \n" + errorList).setType(recordType).setTime(recordTime).setId(UUID.randomUUID().toString()));
                    } else {
                        Record newRecord = new StandardRecord().setFields(map).setType(recordType).setTime(recordTime).setId(UUID.randomUUID().toString());
                        oringinRecords.add(newRecord);
                    }
                }
                if (keepParent) {
                    oringinRecords.add(record);
                }
            }
        } catch (Throwable t) {
            getLogger().error("error while processing records ", t);
        }
        return oringinRecords;
    }

    private List<String> getFieldsNames (String fields) {
        String[] field = fields.split(",");
        List<String> s1 = new ArrayList<>() ;
        for (String s : field) {
            s1.add(s.replaceAll(" ", "")) ;
        }
        return s1;
    }

}
