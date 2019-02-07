package com.hurence.logisland.service;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockServiceController extends AbstractControllerService {

    private static Logger logger = LoggerFactory.getLogger(MockServiceController.class);
    public static final PropertyDescriptor FAKE_SETTINGS = new PropertyDescriptor.Builder()
            .name("fake.settings")
            .description("")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("oups")
            .build();

    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FAKE_SETTINGS);

        return Collections.unmodifiableList(descriptors);
    }
}