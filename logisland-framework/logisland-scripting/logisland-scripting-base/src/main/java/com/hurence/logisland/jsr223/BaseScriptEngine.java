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
package com.hurence.logisland.jsr223;

import java.io.IOException;
import java.io.Reader;
import java.util.Objects;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

public abstract class BaseScriptEngine extends AbstractScriptEngine {

	protected final static String getString(Reader reader) throws IOException {
		Objects.requireNonNull(reader);
		
	    int read;
	    StringBuilder builder = new StringBuilder();
	    while ((read = reader.read()) != -1) {
	        builder.append((char) read);
	    }
	    return builder.toString();
	}
	
	private final ScriptEngineFactory scriptEngineFactory;
	
	public BaseScriptEngine(ScriptEngineFactory scriptEngineFactory) {
		Objects.requireNonNull(scriptEngineFactory);
		
		this.scriptEngineFactory = scriptEngineFactory;
	}

	@Override
	public Bindings createBindings() {
		return new SimpleBindings();
	}

	@Override
	public Object eval(Reader reader, ScriptContext context) throws ScriptException {
		try {
			return eval(getString(reader), context);
		} 
		catch (IOException e) {
			throw new ScriptException(e);
		}
	}

	@Override
	public ScriptEngineFactory getFactory() {
		return scriptEngineFactory;
	}
	
}
