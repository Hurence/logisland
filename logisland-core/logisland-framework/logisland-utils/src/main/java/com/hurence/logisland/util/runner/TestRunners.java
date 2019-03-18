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
package com.hurence.logisland.util.runner;


import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestRunners {

    private static Logger logger = LoggerFactory.getLogger(TestRunners.class);

    public static TestRunner newTestRunner(final Processor processor) {
        return new StandardProcessorTestRunner(processor);
    }

    public static TestRunner newTestRunner(final ProcessContext processContext) {
        return new StandardProcessorTestRunner(processContext);
    }

    public static TestRunner newTestRunner(final String processorClass) {
        try {
            return newTestRunner((Processor)
                    ComponentFactory.loadComponent(processorClass)
            );
        } catch (final Exception e) {
            logger.error("Could not instantiate instance of class " + processorClass + " due to: " + e);
            throw new RuntimeException(e);
        }
    }

}
