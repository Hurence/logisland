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
package com.hurence.logisland.component;

import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.expressionlanguage.InterpreterEngine;
import com.hurence.logisland.expressionlanguage.InterpreterEngineException;
import com.hurence.logisland.expressionlanguage.InterpreterEngineFactory;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.registry.VariableRegistry;

import javax.script.*;


/**
 * Implementation of PropertyValue interface for configuration parameters that support Expression Language.
 */
public class InterpretedPropertyValue extends AbstractPropertyValue {

    private String script;
    private CompiledScript compiledScript;


    public InterpretedPropertyValue(final PropertyDescriptor descriptor, final String rawValue, final ControllerServiceLookup serviceLookup,
                                    final VariableRegistry variableRegistry) {

        this.rawValue = rawValue;
        this.serviceLookup = serviceLookup;
        this.variableRegistry = variableRegistry;

        InterpreterEngine ie = InterpreterEngineFactory.get();
        if (ie.isCompilable()) {
            try {
                this.compiledScript = ie.compile(InterpreterEngine.extractExpressionLanguage(rawValue));
                this.script = null;

            } catch (InterpreterEngineException e) {
                throw new RuntimeException(e);
            }
        } else {
            this.compiledScript = null;
            this.script = rawValue;
        }
    }

    @Override
    public PropertyValue evaluate(Record record) {
        return new DecoratedInterpretedPropertyValue(this, record);
    }

    @Override
    public String getRawValue() {
        throw new RuntimeException("When reading a property with expression language enabled, you must call the evaluate(record) method first");
    }

    /**
     * Method that applies a record to an expression language
     * @param record
     * @return
     * @throws InterpreterEngineException
     */
    protected Object getRawValue(Record record) throws InterpreterEngineException {
        ScriptContext context = new SimpleScriptContext();
        record.getFieldsEntrySet().forEach(entry -> context.setAttribute(entry.getKey(), entry.getValue().getRawValue(), ScriptContext.ENGINE_SCOPE));
        return getRawValue(context);
    }

    /**
     * Method that executes the expression language (either compiled of interpreted) using the provided scope.
     * @param context
     * @return
     * @throws InterpreterEngineException
     */
    private Object getRawValue(ScriptContext context) throws InterpreterEngineException {
        if (compiledScript != null) {
            try {
                return compiledScript.eval(context);
            } catch (ScriptException se) {
                throw new InterpreterEngineException(se);
            }
        } else {
            return InterpreterEngineFactory.get().process(script, context);
        }
    }

}
