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
package com.hurence.logisland.rest.processor.lookup;


import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.ExtendedJsonSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.SerializerProvider;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractCallRequest extends AbstractHttpProcessor
{

    public static final PropertyDescriptor FIELD_HTTP_RESPONSE = new PropertyDescriptor.Builder()
            .name("field.http.response")
            .description("The name of the field to put http response")
            .required(false)
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor REQUEST_METHOD = new PropertyDescriptor.Builder()
            .name("request.method")
            .description("The HTTP VERB Request to use.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor REQUEST_MIME_TYPE = new PropertyDescriptor.Builder()
            .name("request.mime.type")
            .description("The response mime type expected for the response to use in request.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    /*
            tag1=valuea,valueb;tag2=valuea,valuec
     */
    public static final PropertyDescriptor TAG_KEY_VALUE = new PropertyDescriptor.Builder()
            .name("tag.map")
            .description("the tags from the record with their values to allow the bulk rest call")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();



    public static final PropertyDescriptor REQUEST_BODY = new PropertyDescriptor.Builder()
            .name("request.body")
            .description("The body to use for the request.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor INPUT_AS_BODY = new PropertyDescriptor.Builder()
            .name("input.as.body")
            .description("If the input record should be serialized into json and used as body of request or not.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final String OVERWRITE_EXISTING_VALUE = "overwrite_existing";
    public static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue(OVERWRITE_EXISTING_VALUE, "overwrite existing field", "if field already exist");
    public static final String KEEP_OLD_FIELD_VALUE = "keep_only_old_field";
    public static final AllowableValue KEEP_OLD_FIELD =
            new AllowableValue(KEEP_OLD_FIELD_VALUE, "keep only old field value", "keep only old field");
    public static final String IGNORE_RESPONSE_VALUE = "ignore_response_field";
    public static final AllowableValue IGNORE_RESPONSE =
            new AllowableValue(IGNORE_RESPONSE_VALUE, "discard/ignore response", "discard/ignore response");

    public static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
            .name("conflict.resolution.policy")
            .description("What to do when a field with the same name already exists ?")
            .required(false)
            .defaultValue(KEEP_OLD_FIELD.getValue())
            .allowableValues(OVERWRITE_EXISTING, KEEP_OLD_FIELD, IGNORE_RESPONSE)
            .build();

    public static final PropertyDescriptor VALID_HTTP_CODES = new PropertyDescriptor.Builder()
            .name("valid.http.response")
            .description("A comma separated list of integer (http codes)." +
                    "If not specified return every response. If specified add error for responses with http code not listed.")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_OF_INTEGER_VALIDATOR)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEEP_ONLY_BODY_RESPONSE = new PropertyDescriptor.Builder()
            .name("keep.only.response.body")
            .description("if set only keeps body returned value instead of entire http response with http code and http message.")
            .required(false)
            .defaultValue("false")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    Set<Integer> validHttpCodes = new HashSet<>();
    String responseFieldName;
    String conflictPolicy;
    boolean inputAsBody;
    boolean onlyKeepResponseBody;
    RecordSerializer serializer;
    
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        if (context.getPropertyValue(INPUT_AS_BODY).asBoolean() && context.getPropertyValue(REQUEST_BODY).isSet()) {
            validationResults.add(
                    new ValidationResult.Builder()
                            .input(String.format("properties '%s' and '%s' are mutually exclusive so they can not be set both at the same time.",
                                    INPUT_AS_BODY.getName(), REQUEST_BODY.getName()))
                            .valid(false)
                            .build());
        }
        if (context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString().equals(IGNORE_RESPONSE.getValue()) && context.getPropertyValue(FIELD_HTTP_RESPONSE).isSet()) {
            validationResults.add(
                    new ValidationResult.Builder()
                            .input(String.format("property '%s' can not be set to '%s' when property '%s' is set.",
                                    CONFLICT_RESOLUTION_POLICY.getName(), IGNORE_RESPONSE.getValue(), FIELD_HTTP_RESPONSE.getName()))
                            .valid(false)
                            .build());
        }
        return validationResults;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HTTP_CLIENT_SERVICE);
        props.add(FIELD_HTTP_RESPONSE);
        props.add(REQUEST_MIME_TYPE);
        props.add(REQUEST_METHOD);
        props.add(REQUEST_BODY);
        props.add(INPUT_AS_BODY);
        props.add(CONFLICT_RESOLUTION_POLICY);
        props.add(VALID_HTTP_CODES);
        props.add(KEEP_ONLY_BODY_RESPONSE);
        props.add(TAG_KEY_VALUE);
        return Collections.unmodifiableList(props);
    }

    @Override
    public void init(ProcessContext context) throws InitializationException {
        super.init(context);
        try {
            if (context.getPropertyValue(FIELD_HTTP_RESPONSE).isSet()) {
                this.responseFieldName = context.getPropertyValue(FIELD_HTTP_RESPONSE).asString();
            }
            this.conflictPolicy = context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString();
            this.inputAsBody = context.getPropertyValue(INPUT_AS_BODY).asBoolean();
            this.onlyKeepResponseBody = context.getPropertyValue(KEEP_ONLY_BODY_RESPONSE).asBoolean();
            if (inputAsBody) {
                serializer = SerializerProvider.getSerializer(ExtendedJsonSerializer.class.getName(), null);
            }
            if (context.getPropertyValue(VALID_HTTP_CODES).isSet()) {
                context.getPropertyValue(VALID_HTTP_CODES).asStringOpt().ifPresent(s -> {
                    List<Integer> httpCodes = Arrays
                            .stream(s.split(","))
                            .map(Integer::parseInt)
                            .collect(Collectors.toList());
                    validHttpCodes.addAll(httpCodes);
                });
            } else {
                validHttpCodes.clear();
            }
        } catch (Exception ex) {
            throw new InitializationException(ex);
        }
    }

    void modifyRecord(Record record, Record rsp) {
        if (!validHttpCodes.isEmpty() && !validHttpCodes.contains(rsp.getField(restClientService.getResponseCodeKey()).asInteger())) {
            record.addError(ProcessError.RUNTIME_ERROR.getName(),
                    String.format("http response code was not valid (%s)", validHttpCodes.toString()));
        } else {
            switch (conflictPolicy) {
                case OVERWRITE_EXISTING_VALUE:
                    break;
                case IGNORE_RESPONSE_VALUE:
                    return;
                case KEEP_OLD_FIELD_VALUE:
                    if (record.hasField(responseFieldName)) return;
            }
            if (onlyKeepResponseBody) {
                Field body = rsp.getField(restClientService.getResponseBodyKey());
                record.setField(responseFieldName, body.getType(), body.getRawValue());
            } else {
                record.setRecordField(responseFieldName, rsp);
            }
        }
    }

    Optional<String> calculBody(Record record, ProcessContext context) {
        if (context.getPropertyValue(REQUEST_BODY).isSet()) {
            return Optional.ofNullable(context.getPropertyValue(REQUEST_BODY.getName()).evaluate(record).asString());
        }
        return Optional.empty();
    }

    Optional<String> concatBody(Collection<Record> records, ProcessContext context) {
        StringBuffer result = new StringBuffer();
            records.forEach(record -> {
                if   (triggerRestCall(record,context)) {
                    result.append("{");

                    if (record.getField("ItemId").isSet() && record.getField("Userid").isSet()) {
                        result.append("\"id\":" + Long.parseLong(record.getField("Userid").asString() + record.getField("ItemId").asString()) + ",");
                    }

                    if (record.getField("ItemId").isSet()) {
                        result.append("\"presentationId\":" + record.getField("ItemId").asString() + ",");
                    }
                    if (record.getField("SecondsViewed").isSet()) {
                        result.append("\"timeWatched\":" + record.getField("SecondsViewed").asLong() + ",");
                    }
                    if (record.getField("Userid").isSet()) {
                        result.append("\"userId\":" + record.getField("Userid").asLong() + ",");
                    }
                    if (record.getField("VideoPercentViewed").isSet()) {
                        result.append("\"watched\":" + record.getField("VideoPercentViewed").asInteger());
                    }

                    result.append("},");
                }
            } ) ;
            if (result.length() >0 ) {
            result.setLength(result.length()-1);
            return Optional.ofNullable("[ " + result + " ]");
            }else{
                return Optional.empty();
            }
    }

    Optional<String> calculMimTyp(Record record, ProcessContext context) {
        if (context.getPropertyValue(REQUEST_MIME_TYPE).isSet()) {
            return Optional.ofNullable(context.getPropertyValue(REQUEST_MIME_TYPE.getName()).evaluate(record).asString());
        }
        return Optional.empty();
    }

    Optional<String> calculVerb(Record record, ProcessContext context) {
        if (context.getPropertyValue(REQUEST_METHOD).isSet()) {
            return Optional.ofNullable(context.getPropertyValue(REQUEST_METHOD.getName()).evaluate(record).asString());
        }
        return Optional.empty();
    }

        /*
            tag1=valuea,valueb;tag2=valuea,valuec
            if the record contains one of the tag of the property with one of the value for this tag it will return true
     */
    Boolean triggerRestCall(Record record, ProcessContext context) {
        List<Boolean> resultContainer = new ArrayList<>();
        if (context.getPropertyValue(TAG_KEY_VALUE).isSet()) {
           String tag_list = context.getPropertyValue(TAG_KEY_VALUE).asString();
            String [] keyValuePairs = tag_list.split(";");
            Map<String,String> map = new HashMap<>();
            for(String pair : keyValuePairs)                        //iterate over the pairs
            {
                String[] entry = pair.split("=");                   //split the pairs to get key and value
                map.put(entry[0].trim(), entry[1].trim());          //add them to the hashmap and trim whitespaces
            }
            map.forEach( (k,v) -> { //k here is tag1 and tag2
                if ( record.getField(k) != null && record.getField(k).isSet()){
                    String[] single_values = v.split(",");
                    List<String> stringList = new ArrayList<>(Arrays.asList(single_values)); // List(valuea,valueb) for tag1 List(valuea,valuec) for tag2
                    String recordValue = record.getField(k).asString();
                    resultContainer.add(stringList.contains(recordValue));
                }
            });
        }
        return (!resultContainer.isEmpty() && resultContainer.contains(true));
    }
}