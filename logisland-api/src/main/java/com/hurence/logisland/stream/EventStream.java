package com.hurence.logisland.stream;

import com.hurence.logisland.processor.RecordProcessor;

import java.util.List;

/**
 * Created by tom on 01/07/16.
 */
public interface EventStream {


    List<RecordProcessor> getProcessors();
    String getDescription();
    String getVersion();
}
