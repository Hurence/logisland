package com.hurence.logisland.agent.api;

import com.hurence.logisland.agent.api.model.DataFlow;

import java.util.List;
import java.util.Map;

public interface DataflowApi  {
    //notifyDataflowConfiguration
    public void notifyDataflowConfiguration(String dataflowName,String jobId,DataFlow dataflow);
    
    //pollDataflowConfiguration
    public DataFlow pollDataflowConfiguration(String dataflowName,String ifModifiedSince);
    
}
