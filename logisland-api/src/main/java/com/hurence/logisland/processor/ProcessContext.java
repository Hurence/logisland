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
package com.hurence.logisland.processor;


import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceLookup;

import java.io.Closeable;

public interface ProcessContext extends ComponentContext {

    /**
     * Adds the given {@link ControllerServiceLookup}  so that the
     * configured Processor can access it
     *
     * @param controllerServiceLookup the service
     * @throws InitializationException ie
     */
    void addControllerServiceLookup(ControllerServiceLookup controllerServiceLookup) throws InitializationException;

    Processor getProcessor();
}
