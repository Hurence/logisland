package com.hurence.logisland.stream;

import com.hurence.logisland.processor.EventProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 01/07/16.
 */
public class StandardEventStream implements EventStream {

    private List<EventProcessor> processors = new ArrayList<>();

    public StandardEventStream() {
    }

    @Override
    public List<EventProcessor> getProcessors() {
        return processors;
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public String getVersion() {
        return null;
    }
}
