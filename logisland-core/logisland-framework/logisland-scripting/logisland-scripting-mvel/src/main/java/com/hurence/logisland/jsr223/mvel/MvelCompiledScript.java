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
package com.hurence.logisland.jsr223.mvel;

import java.io.Serializable;
import java.util.Objects;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.mvel2.MVEL;
import org.mvel2.integration.VariableResolverFactory;
import org.mvel2.integration.impl.MapVariableResolverFactory;

public class MvelCompiledScript extends CompiledScript {

	private String expression;
	private Serializable compiledExpression;
	private ScriptEngine scriptEngine;
	
	public MvelCompiledScript(ScriptEngine engine, String expression) {
		Objects.requireNonNull(engine);
		Objects.requireNonNull(expression);
		
		this.scriptEngine = engine;
		this.expression = expression.trim();
		if (this.expression.length()==0) {
			throw new IllegalArgumentException("expression should not be empty" );
		}
		this.compiledExpression = MVEL.compileGetExpression(this.expression);
	}
	
	@Override
	public Object eval(ScriptContext context) throws ScriptException {
		try {
			Bindings map = context.getBindings(ScriptContext.ENGINE_SCOPE);
			VariableResolverFactory functionFactory = new MapVariableResolverFactory(map);
			return MVEL.executeExpression(compiledExpression, map, functionFactory);
		}
		catch (Throwable t) {
			return null;
		}
	}

	@Override
	public ScriptEngine getEngine() {
		return scriptEngine;
	}

}
