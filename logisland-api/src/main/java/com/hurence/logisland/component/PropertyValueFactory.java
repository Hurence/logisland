/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.component;

import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.expressionlanguage.InterpreterEngine;
import com.hurence.logisland.expressionlanguage.InterpreterEngineException;
import com.hurence.logisland.expressionlanguage.InterpreterEngineFactory;
import com.hurence.logisland.registry.VariableRegistry;

/**
 * Factory that builds the right type of PropertyValue object.
 */
public class PropertyValueFactory {

    public static PropertyValue getInstance(
            final PropertyDescriptor descriptor,
            final String rawValue,
            final ControllerServiceLookup controllerServiceLookup,
            final VariableRegistry variableRegistry) {
        if (descriptor.isExpressionLanguageSupported() && InterpreterEngine.isExpressionLanguage(rawValue)) {
            InterpreterEngine ie = InterpreterEngineFactory.get();

            return new InterpretedPropertyValue(descriptor, rawValue, controllerServiceLookup, variableRegistry);

        }

        return new StandardPropertyValue(rawValue, controllerServiceLookup, variableRegistry);
    }

    public static PropertyValue getInstance(
            final PropertyDescriptor descriptor,
            final String rawValue,
            final ControllerServiceLookup controllerServiceLookup) {
        return getInstance(descriptor, rawValue, controllerServiceLookup, VariableRegistry.EMPTY_REGISTRY);
    }
}
