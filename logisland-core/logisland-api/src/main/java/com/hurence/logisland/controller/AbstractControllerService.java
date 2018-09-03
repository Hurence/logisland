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

package com.hurence.logisland.controller;


import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.AbstractConfigurableComponent;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.logging.ComponentLog;

import java.io.IOException;

public abstract class AbstractControllerService extends AbstractConfigurableComponent implements ControllerService {


    private ControllerServiceLookup serviceLookup;
    private ComponentLog logger;
    private volatile boolean enabled = true;

    @Override
    public final void initialize(final ControllerServiceInitializationContext context) throws InitializationException {
        this.identifier = context.getIdentifier();
        serviceLookup = context.getControllerServiceLookup();
        logger = context.getLogger();
        try {
            init(context);
        } catch (IOException e) {
            throw  new InitializationException(e);
        }
    }


    /**
     * @return the {@link ControllerServiceLookup} that was passed to the
     * {@link #init(ControllerServiceInitializationContext)} method
     */
    protected final ControllerServiceLookup getControllerServiceLookup() {
        return serviceLookup;
    }

    /**
     * Provides a mechanism by which subclasses can perform initialization of
     * the Controller Service before it is scheduled to be run
     *
     * @param context of initialization context
     * @throws InitializationException if unable to init
     */
    protected void init(final ControllerServiceInitializationContext context) throws InitializationException, IOException {
    }

    /**
     * @return the logger that has been provided to the component by the
     * framework in its initialize method
     */
    protected ComponentLog getLogger() {
        return logger;
    }


    @OnEnabled
    public final void enabled() {
        this.enabled = true;
    }

    @OnDisabled
    public final void disabled() {
        this.enabled = false;
    }

    public boolean isEnabled() {
        return this.enabled;
    }
}
