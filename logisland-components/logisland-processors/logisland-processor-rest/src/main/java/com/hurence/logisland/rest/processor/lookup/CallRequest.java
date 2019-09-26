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
import com.hurence.logisland.record.RecordUtils;
import com.hurence.logisland.rest.service.lookup.RestLookupService;
import com.hurence.logisland.service.lookup.LookupFailureException;
import com.hurence.logisland.validator.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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

    private static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist");

    private static final AllowableValue KEEP_OLD_FIELD =
            new AllowableValue("keep_only_old_field", "keep only old field value", "keep only old field");


    private static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
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
            String verb = calculVerb(record, context);
            String mimeType = calculMimTyp(record, context);
            String body = calculBody(record, context);
            Record coordinates = RecordUtils.getRecordOfString(
                    RestLookupService.BODY_KEY, body,
                    RestLookupService.MIME_TYPE_KEY, mimeType,
                    RestLookupService.METHOD_KEY, verb
            );
            try {
                restClientService.lookup(coordinates).ifPresent(rsp -> {
                    record.setRecordField(responseFieldName, rsp);
                });
            } catch (LookupFailureException ex) {
                record.addError(ProcessError.RUNTIME_ERROR.getName(), getLogger(), ex.getMessage());
            }
        }
        return records;
    }

    private String calculBody(Record record, ProcessContext context) {
//        if (context.getPropertyValue(REQUEST_BODY).isSet()) {
        return context.getPropertyValue(context.getProperty(REQUEST_BODY)).evaluate(record).asString();
//        }
//        return RestLookupService.BO
    }

    private String calculMimTyp(Record record, ProcessContext context) {
        return context.getPropertyValue(context.getProperty(REQUEST_MIME_TYPE)).evaluate(record).asString();
    }

    private String calculVerb(Record record, ProcessContext context) {
        return context.getPropertyValue(context.getProperty(REQUEST_METHOD)).evaluate(record).asString();
    }
}