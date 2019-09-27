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

    String getMimeTypeKey();

    String getMethodKey();

    String getbodyKey();
}

