package com.hurence.logisland.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 01/07/16.
 */
public class EventStreamConfig {

    private String documentation = "";
    private String version = "";
    private List<EventProcessorConfig> processorConfigs = new ArrayList<>();
}
