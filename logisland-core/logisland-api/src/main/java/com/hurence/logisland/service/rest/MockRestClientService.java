package com.hurence.logisland.service.rest;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.lookup.LookupFailureException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A mock RestClient Services that return back the gicen coordinates
 */
public class MockRestClientService extends AbstractControllerService implements RestClientService {

    public static String fakeBody;

    public MockRestClientService() {
    }

    public MockRestClientService(String fakeBody) {
        this.fakeBody = fakeBody;
    }

    @Override
    public String getMimeTypeKey() {
        return "request.mime";
    }

    @Override
    public String getMethodKey() {
        return "request.method";
    }

    @Override
    public String getbodyKey() {
        return "request.body";
    }

    @Override
    public String getResponseCodeKey() {
        return "code";
    }

    @Override
    public String getResponseMsgCodeKey() {
        return "code.message";
    }

    @Override
    public String getResponseBodyKey() {
        return "body";
    }

    @Override
    public Optional<Record> lookup(Record coordinates) throws LookupFailureException {
        Record response = new StandardRecord(coordinates);
        if (fakeBody != null) {
            response.setStringField(getResponseBodyKey(), fakeBody);
        }
        return Optional.of(response);
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.emptyList();
    }
}
