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
package com.hurence.logisland.jsr223.mvel;

import java.io.IOException;
import java.io.Reader;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;

import com.hurence.logisland.jsr223.BaseScriptEngine;

public class MvelScriptEngine extends BaseScriptEngine implements Compilable  {

	public MvelScriptEngine(ScriptEngineFactory scriptEngineFactory) {
		super(scriptEngineFactory);
	}

	@Override
	public CompiledScript compile(Reader script) throws ScriptException {
		try {
			return compile(getString(script));
		} 
		catch (IOException e) {
			throw new ScriptException(e);
		}
	}

	@Override
	public CompiledScript compile(String script) throws ScriptException {
		return new MvelCompiledScript(this, script);
	}

	@Override
	public Object eval(String script, ScriptContext context) throws ScriptException {
		return compile(script).eval(context);
	}
	
}
