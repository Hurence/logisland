/*
 * Copyright (C) 2019 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.engine.vanilla;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.controller.StandardControllerServiceLookup;
import com.hurence.logisland.engine.AbstractProcessingEngine;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.stream.AbstractRecordStream;
import com.hurence.logisland.stream.StreamContext;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class PlainJavaEngine extends AbstractProcessingEngine {

    private final ComponentLog logger = getLogger();

    private CountDownLatch countDownLatch = new CountDownLatch(0);

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    public void start(EngineContext engineContext) {
        logger.info("Starting");
        ControllerServiceLookup controllerServiceLookup = new StandardControllerServiceLookup(engineContext.getControllerServiceConfigurations());

        for (StreamContext streamContext : engineContext.getStreamContexts()) {
            try {
                streamContext.setControllerServiceLookup(controllerServiceLookup);
                ((AbstractRecordStream) streamContext.getStream()).init(streamContext);
                streamContext.getStream().start();
            } catch (Exception e) {
                throw new IllegalStateException("Unable to start engine", e);
            }
        }
        countDownLatch = new CountDownLatch(engineContext.getStreamContexts().size());
        logger.info("Started");


    }

    @Override
    public void shutdown(EngineContext engineContext) {
        logger.info("Stopping");
        engineContext.getStreamContexts().forEach(streamContext -> {
            try {
                streamContext.getStream().stop();
            } catch (Throwable t) {
                logger.warn("Error stopping stream " + streamContext.getStream().getIdentifier(), t);
            } finally {
                countDownLatch.countDown();
            }
        });
        logger.info("Stopped");

    }

    @Override
    public void awaitTermination(EngineContext engineContext) {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting");
        }
    }

    @Override
    public void reset(EngineContext engineContext) {
        shutdown(engineContext);
    }
}
