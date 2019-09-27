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
    public Optional<Record> lookup(Record coordinates) throws LookupFailureException {
        return Optional.of(new StandardRecord(coordinates));
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
