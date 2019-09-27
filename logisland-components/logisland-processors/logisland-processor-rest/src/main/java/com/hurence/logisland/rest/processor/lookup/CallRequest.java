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


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.error.ErrorUtils;

import java.util.*;

@Tags({"datastore", "record", "put", "bulk"})
@CapabilityDescription("Indexes the content of a Record in a Datastore using bulk processor")
@ExtraDetailFile("./details/common-processors/BulkPut-Detail.rst")
public class CallRequest extends AbstractHttpProcessor
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
//            .defaultValue("get")
            .build();

    public static final PropertyDescriptor REQUEST_MIME_TYPE = new PropertyDescriptor.Builder()
            .name("request.mime.type")
            .description("The response mime type expected for the response to use in request.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
//            .defaultValue("text/plain")
            .build();

    public static final PropertyDescriptor REQUEST_BODY = new PropertyDescriptor.Builder()
            .name("request.body")
            .description("The body to use for the request.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

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

    private String responseFieldName = null;
    private String conflictPolicy = null;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HTTP_CLIENT_SERVICE);
        props.add(FIELD_HTTP_RESPONSE);
        props.add(REQUEST_MIME_TYPE);
        props.add(REQUEST_METHOD);
        props.add(REQUEST_BODY);
        props.add(CONFLICT_RESOLUTION_POLICY);
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
        } catch (Exception ex) {
            throw new InitializationException(ex);
        }
    }

    /**
     * process events
     *
     * @param context
     * @param records
     * @return
     */
    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> records) {
        if (records.isEmpty()) {
            getLogger().warn("process has been called with an empty list of records !");
            return records;
        }
        /**
         * loop over events to add them to bulk
         */
        for (Record record : records) {
            StandardRecord coordinates = new StandardRecord(record);
            calculVerb(record, context).ifPresent(verb -> coordinates.setStringField(restClientService.getMethodKey(), verb));
            calculMimTyp(record, context).ifPresent(mimeType -> coordinates.setStringField(restClientService.getMimeTypeKey(), mimeType));
            calculBody(record, context).ifPresent(body -> coordinates.setStringField(restClientService.getbodyKey(), body));
            try {
                restClientService.lookup(coordinates).ifPresent(rsp -> {
                    record.setRecordField(responseFieldName, rsp);
                });
            } catch (Exception ex) { //There is other errors than LookupException, The proxyWrapper does wrap those into Reflection exceptions...
                ErrorUtils.handleError(getLogger(), ex, record, ProcessError.RUNTIME_ERROR.getName());
            }
        }
        return records;
    }

    private Optional<String> calculBody(Record record, ProcessContext context) {
        if (context.getPropertyValue(REQUEST_BODY).isSet()) {
            return Optional.ofNullable(context.getPropertyValue(REQUEST_BODY.getName()).evaluate(record).asString());
        }
        return Optional.empty();
    }

    private Optional<String> calculMimTyp(Record record, ProcessContext context) {
        if (context.getPropertyValue(REQUEST_MIME_TYPE).isSet()) {
            return Optional.ofNullable(context.getPropertyValue(REQUEST_MIME_TYPE.getName()).evaluate(record).asString());
        }
        return Optional.empty();
    }

    private Optional<String> calculVerb(Record record, ProcessContext context) {
        if (context.getPropertyValue(REQUEST_METHOD).isSet()) {
            return Optional.ofNullable(context.getPropertyValue(REQUEST_METHOD.getName()).evaluate(record).asString());
        }
        return Optional.empty();
    }
}