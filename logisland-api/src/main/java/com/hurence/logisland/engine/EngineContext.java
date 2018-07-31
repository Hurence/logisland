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
package com.hurence.logisland.engine;


import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.stream.StreamContext;

import java.util.Collection;

public interface EngineContext extends ComponentContext {

    /**
     * @return retrieve the list of stream contexts
     */
    Collection<StreamContext> getStreamContexts();

    /**
     * add a stream to the collection of Streams
     *
     * @param streamContext the Stream to add
     */
    void addStreamContext(StreamContext streamContext);


    /**
     * @return the engine
     */
    ProcessingEngine getEngine();


    /**
     * @return the init context for controllers
     */
    Collection<ControllerServiceConfiguration> getControllerServiceConfigurations();

    /**
     * add a ControllerServiceConfiguration
     *
     * @param config to add
     */
    void addControllerServiceConfiguration(ControllerServiceConfiguration config);


}
