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
package com.hurence.logisland.service.rest;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.service.lookup.RecordLookupService;
import com.hurence.logisland.validator.StandardValidators;

public interface RestClientService extends RecordLookupService {

    PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("rest.lookup.url")
            .displayName("URL")
            .description("The URL for the REST endpoint. Expression language is evaluated against the lookup key/value pairs")
            .expressionLanguageSupported(false)
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    /*
    Request keys
    */
    String getMimeTypeKey();

    String getMethodKey();

    String getbodyKey();

    /*
    Response keys
    */
    String getResponseCodeKey();

    String getResponseMsgCodeKey();

    String getResponseBodyKey();
}

