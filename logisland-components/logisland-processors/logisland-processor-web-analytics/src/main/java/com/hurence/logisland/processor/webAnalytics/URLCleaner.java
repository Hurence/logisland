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
package com.hurence.logisland.processor.webAnalytics;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.processor.webAnalytics.modele.AllQueryParameterRemover;
import com.hurence.logisland.processor.webAnalytics.modele.KeepSomeQueryParameterRemover;
import com.hurence.logisland.processor.webAnalytics.modele.QueryParameterRemover;
import com.hurence.logisland.processor.webAnalytics.modele.RemoveSomeQueryParameterRemover;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.*;

@Tags({"record", "fields", "Decode"})
@CapabilityDescription("Decode one or more field containing an URL with possibly special chars encoded\n" +
        "...")
@DynamicProperty(name = "fields to decode",
        supportsExpressionLanguage = false,
        value = "a default value",
        description = "Decode one or more fields from the record ")
@ExtraDetailFile("./details/URLDecoder-Detail.rst")
public class URLCleaner extends AbstractProcessor {

    public static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist");

    public static final AllowableValue KEEP_OLD_FIELD =
            new AllowableValue("keep_only_old_field", "keep only old field value", "keep only old field");

    public static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
            .name("conflict.resolution.policy")
            .description("What to do when a field with the same name already exists ?")
            .required(false)
            .defaultValue(KEEP_OLD_FIELD.getValue())
            .allowableValues(OVERWRITE_EXISTING, KEEP_OLD_FIELD)
            .build();

    public static final PropertyDescriptor URL_FIELDS = new PropertyDescriptor.Builder()
            .name("url.fields")
            .description("List of fields (URL) to decode and optionnaly the output field for the url modified. Syntax should be " +
                    "<name>,<name:newName>,...,<name>. So fields name can not contain ',' nor ':'")
            .required(true)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_COLON_SUB_SEPARATOR_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final String PARAM_NAMES_INCLUDE_PROP_NAME = "param.names.include";
    public static final String REMOVE_PARAMS_PROP_NAME = "param.names.exclude";
    public static final String REMOVE_ALL_PARAMS_PROP_NAME = "remove.all.params";

    public static final PropertyDescriptor KEEP_PARAMS = new PropertyDescriptor.Builder()
            .name(PARAM_NAMES_INCLUDE_PROP_NAME)
            .description("List of param names to keep in the input url (others will be removed). Can not be given at the same time as " +
                    REMOVE_PARAMS_PROP_NAME + " or " + REMOVE_ALL_PARAMS_PROP_NAME)
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOVE_PARAMS = new PropertyDescriptor.Builder()
            .name(REMOVE_PARAMS_PROP_NAME)
            .description("List of param names to remove from the input url (others will be kept). Can not be given at the same time as " +
                    PARAM_NAMES_INCLUDE_PROP_NAME + " or "  + REMOVE_ALL_PARAMS_PROP_NAME)
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOVE_ALL_PARAMS = new PropertyDescriptor.Builder()
            .name(REMOVE_ALL_PARAMS_PROP_NAME)
            .description("Remove all params if true.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private final Map<String, String> fieldsToDecodeToOutputField = new HashMap<>();
    private String conflictPolicy;
    private QueryParameterRemover remover;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(URL_FIELDS);
        descriptors.add(CONFLICT_RESOLUTION_POLICY);
        descriptors.add(KEEP_PARAMS);
        descriptors.add(REMOVE_PARAMS);
        descriptors.add(REMOVE_ALL_PARAMS);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        /**
         * Only one of both properties may be set.
         */
        if (context.getPropertyValue(REMOVE_ALL_PARAMS).isSet()) {
            if (context.getPropertyValue(REMOVE_ALL_PARAMS).asBoolean()) {
                if (context.getPropertyValue(KEEP_PARAMS).isSet() || context.getPropertyValue(REMOVE_PARAMS).isSet())
                {
                    validationResults.add(
                            new ValidationResult.Builder()
                                    .explanation(KEEP_PARAMS.getName() + " and " + REMOVE_PARAMS.getName() +
                                            " properties are mutually exclusive and can not be set if " + REMOVE_ALL_PARAMS.getName() + " is set to true")
                                    .valid(false)
                                    .build());
                }
            } else {
                if (!context.getPropertyValue(KEEP_PARAMS).isSet() && !context.getPropertyValue(REMOVE_PARAMS).isSet()) {
                    validationResults.add(
                            new ValidationResult.Builder()
                                    .explanation(KEEP_PARAMS.getName() + " or " + REMOVE_PARAMS.getName() +
                                            " properties is required when " + REMOVE_ALL_PARAMS.getName() + " is set to false")
                                    .valid(false)
                                    .build());
                }
            }
        }
        if (context.getPropertyValue(KEEP_PARAMS).isSet() && context.getPropertyValue(REMOVE_PARAMS).isSet())
        {
            validationResults.add(
                    new ValidationResult.Builder()
                            .explanation(KEEP_PARAMS.getName() + " and " + REMOVE_PARAMS.getName() +
                                    " properties are mutually exclusive so it can not be set both at the same time.")
                            .valid(false)
                            .build());
        }
        return validationResults;
    }

    public void init(ProcessContext context) throws InitializationException {
        super.init(context);
        initFieldsToDecodeToOutputFiles(context);
        this.conflictPolicy = context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString();
        initRemover(context);
    }

    public void initFieldsToDecodeToOutputFiles(ProcessContext context) {
        fieldsToDecodeToOutputField.clear();
        String commaSeparatedFields = context.getPropertyValue(URL_FIELDS).asString();
        String[] fieldsArr = commaSeparatedFields.split("\\s*,\\s*");
        for (String field : fieldsArr) {
            if (field.contains(":")) {
                String[] fieldPair = field.split("\\s*:\\s*");
                fieldsToDecodeToOutputField.put(fieldPair[0], fieldPair[1]);
            } else {
                fieldsToDecodeToOutputField.put(field, field);
            }
        }
    }

    public void initRemover(ProcessContext context) throws InitializationException {
        if (context.getPropertyValue(KEEP_PARAMS).isSet()) {
            String commaSeparatedKeepParams = context.getPropertyValue(KEEP_PARAMS).asString();
            String[] keepParamsArr = commaSeparatedKeepParams.split("\\s*,\\s*");
            final Set<String> keepParams = new HashSet<>(Arrays.asList(keepParamsArr));
            this.remover = new KeepSomeQueryParameterRemover(keepParams);
            return;
        }
        if (context.getPropertyValue(REMOVE_PARAMS).isSet()) {
            String commaSeparatedRemoveParam = context.getPropertyValue(REMOVE_PARAMS).asString();
            String[] removeParamsArr = commaSeparatedRemoveParam.split("\\s*,\\s*");
            final Set<String> removeParams = new HashSet<>(Arrays.asList(removeParamsArr));
            this.remover = new RemoveSomeQueryParameterRemover(removeParams);
            return;
        }
        if (!context.getPropertyValue(REMOVE_ALL_PARAMS).isSet() || context.getPropertyValue(REMOVE_ALL_PARAMS).asBoolean()) {
            this.remover = new AllQueryParameterRemover();
        } else {
            throw new InitializationException("No remover was built, should never happen !" +
                    "Problem with configuration checking in processor.");
        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        for (Record record : records) {
            updateRecord(record);
        }
        return records;
    }


    private void updateRecord(Record record) {
        fieldsToDecodeToOutputField.entrySet().forEach(kv -> {
            tryUpdatingRecord(record, kv);
        });
    }

    private void tryUpdatingRecord(Record record, Map.Entry<String, String> kv) {
        String inputFieldName = kv.getKey();
        String outputFieldName = kv.getValue();
        if (record.hasField(inputFieldName)) {
            String value = record.getField(inputFieldName).asString();
            if (value != null) {
                String cleanedUrl = null;
                try {
                    cleanedUrl = remover.removeQueryParameters(value);
                } catch (Exception e) {
                    getLogger().error("Error for url {}, for record {}.", new Object[]{value, record.getId()}, e);
                    String msg = "Could not process url : '" + value + "'.\n Cause: " + e.getMessage();
                    record.addError(ProcessError.STRING_FORMAT_ERROR.toString(), getLogger(), msg);
                    return;
                }
                if (!record.hasField(outputFieldName) || conflictPolicy.equals(OVERWRITE_EXISTING.getValue())) {
                    record.setField(outputFieldName, FieldType.STRING, cleanedUrl);
                }
            }
        }
    }
}
