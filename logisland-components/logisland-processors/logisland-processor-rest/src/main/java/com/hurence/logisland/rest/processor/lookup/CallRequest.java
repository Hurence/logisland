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
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.error.ErrorUtils;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.serializer.ExtendedJsonSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.SerializerProvider;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.*;

@Tags({"rest", "record", "http", "request", "call", "server"})
@CapabilityDescription("Execute an http request with specified verb, body and mime type. Then stock result as a Record in the specified field")
//@ExtraDetailFile("./details/common-processors/BulkPut-Detail.rst")
public class CallRequest extends AbstractCallRequest
{
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
            if (inputAsBody) {
                OutputStream out = new ByteArrayOutputStream();
                serializer.serialize(out, record);
                coordinates.setStringField(restClientService.getbodyKey(), out.toString());
            } else {
                calculBody(record, context).ifPresent(body -> coordinates.setStringField(restClientService.getbodyKey(), body));
            }
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
}