/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
