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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import org.mvel2.MVEL;

public final class MvelScriptEngineFactory implements ScriptEngineFactory {
	private final static String NAME = "Mvel";
	private final static String VERSION = "1.0";
	private static final List<String> EXTENSIONS = Collections.singletonList(NAME.toLowerCase());

	public MvelScriptEngineFactory() {
	}
	
	@Override
	public String getEngineName() {
		return NAME;
	}

	@Override
	public String getEngineVersion() {
		return VERSION;
	}

	@Override
	public List<String> getExtensions() {
		return EXTENSIONS;
	}

	@Override
	public String getLanguageName() {
		return MVEL.NAME;
	}

	@Override
	public String getLanguageVersion() {
		return MVEL.VERSION;
	}

	@Override
	public String getMethodCallSyntax(String obj, String m, String... args) {
		return null;
	}

	@Override
	public List<String> getMimeTypes() {
		return null;
	}

	@Override
	public List<String> getNames() {
		return Arrays.asList(NAME, NAME.toLowerCase(), NAME.toUpperCase(), MVEL.CODENAME);
	}

	@Override
	public String getOutputStatement(String toDisplay) {
		return null;
	}

	@Override
	public Object getParameter(String key) {
		return null;
	}

	@Override
	public String getProgram(String... statements) {
		return null;
	}

	@Override
	public ScriptEngine getScriptEngine() {
		return new MvelScriptEngine(this);
	}

}
