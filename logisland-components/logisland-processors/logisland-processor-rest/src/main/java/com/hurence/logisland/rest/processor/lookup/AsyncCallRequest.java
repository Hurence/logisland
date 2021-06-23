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
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.error.ErrorUtils;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import io.reactivex.Maybe;
import io.vertx.core.Handler;
import io.vertx.reactivex.core.Promise;
import io.vertx.reactivex.core.Vertx;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Tags({"rest", "record", "http", "request", "call", "server"})
@CapabilityDescription("Execute an http request with specified verb, body and mime type. Then stock result as a Record in the specified field")
//@ExtraDetailFile("./details/common-processors/BulkPut-Detail.rst")
public class AsyncCallRequest extends AbstractCallRequest
{
    private Vertx vertx;

    @Override
    public void init(ProcessContext context) throws InitializationException {
        super.init(context);
        try {
            vertx = Vertx.vertx();
        } catch (Exception ex) {
            throw new InitializationException(ex);
        }
    }

    public void stop() {
        if (vertx != null) {
            vertx.close();
            vertx = null;
            setIsInitialized(false);
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
        List<Maybe<Optional<Record>>> responses = records.stream()
                .filter(record -> triggerRestCall(record, context))
                .map(record -> {
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
                    Handler<Promise<Optional<Record>>> callRequestHandler = p -> {
                        try {
                            p.complete(restClientService.lookup(coordinates));
                        } catch (Throwable t) { //There is other errors than LookupException, The proxyWrapper does wrap those into Reflection exceptions...
                            p.fail(t);
                        }
                    };
//                    return Optional<Record> record;
                    return vertx
                            .rxExecuteBlocking(callRequestHandler)
                            .doOnError(t -> {
                                ErrorUtils.handleError(getLogger(), t, record, ProcessError.RUNTIME_ERROR.getName());
                            })
                            .doOnSuccess(rspOpt -> {
                                rspOpt.ifPresent(rsp ->  modifyRecord(record, rsp));
                            });
                }).collect(Collectors.toList());
        Maybe.<Optional<Record>, Integer>zip(responses, (opts) -> { return 0; }).blockingGet();//wait until all request are done
        return records;
    }
}