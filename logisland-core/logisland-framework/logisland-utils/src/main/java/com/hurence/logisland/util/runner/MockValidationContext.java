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


import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.processor.StandardValidationContext;
import com.hurence.logisland.registry.VariableRegistry;
import com.hurence.logisland.validator.ValidationContext;


public class MockValidationContext extends StandardValidationContext implements ValidationContext {

    final MockProcessContext context;
    final VariableRegistry variableRegistry;

    public MockValidationContext(final MockProcessContext processContext, final VariableRegistry variableRegistry) {
        super(processContext.getProperties());
        this.context = processContext;
        this.variableRegistry = variableRegistry;
    }

    @Override
    public ValidationContext getControllerServiceValidationContext(final ControllerService controllerService) {
        final MockProcessContext serviceProcessContext = new MockProcessContext(controllerService, context, variableRegistry);

        return new MockValidationContext(serviceProcessContext, variableRegistry);
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this.context.getServiceLookup();
    }

}
