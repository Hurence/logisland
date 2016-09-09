package com.hurence.logisland.processor.debug;

import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.event.Event;
import com.hurence.logisland.processor.AbstractEventProcessor;
import com.hurence.logisland.processor.ProcessContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class EventDebuggerProcessor extends AbstractEventProcessor {

    private static Logger logger = LoggerFactory.getLogger(EventDebuggerProcessor.class);


    @Override
    public Collection<Event> process(final ProcessContext context, final Collection<Event> collection) {
        logger.debug("processing {} events", collection.size());
        collection.stream().forEach(event -> logger.debug("processing : {}", event));
        return Collections.emptyList();
    }


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        return Collections.emptyList();
    }

    @Override
    public String getIdentifier() {
        return null;
    }
}
