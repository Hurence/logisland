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

import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.component.InitializationException;

/**
 * Carry the whole workload of processing
 */
public interface ProcessingEngine extends ConfigurableComponent {

    /**
     * Init the engine with a context
     *
     * @param engineContext
     */
    void init(EngineContext engineContext) throws InitializationException;

    /**
     * start the engine with a context
     *
     * @param engineContext
     */
    void start(EngineContext engineContext);

    /**
     * shutdown the engine with a context
     *
     * @param engineContext
     */
    void stop(EngineContext engineContext);


    /**
     * Stop the engine (and all streams in it) but keep reusable
     * ressources without closing them so that it can be used in
     * a next run
     *
     * @param engineContext
     */
    void softStop(EngineContext engineContext);

    /**
     * Await for termination.
     * @param engineContext
     */
    void awaitTermination(EngineContext engineContext);

}
