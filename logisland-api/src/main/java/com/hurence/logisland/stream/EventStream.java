package com.hurence.logisland.stream;

import com.hurence.logisland.processor.EventProcessor;

import java.util.List;

/**
 * Created by tom on 01/07/16.
 */
public interface EventStream {


    List<EventProcessor> getProcessors();
    String getDescription();
    String getVersion();
}
