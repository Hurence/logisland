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
package com.hurence.logisland.jsr223;

import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptContext;

public class ScriptContextImpl implements ScriptContext {

	private static List<Integer> scopes;
	static {
		scopes = new ArrayList<Integer>(2);
		scopes.add(ENGINE_SCOPE);
		scopes.add(GLOBAL_SCOPE);
		scopes = Collections.unmodifiableList(scopes);
	}
	private Bindings engineScope;
	private Writer errorWriter;
	private Bindings globalScope;

	private Reader reader;

	private Writer writer;

	public ScriptContextImpl(ScriptContext parent, Map<String, Object> model) {
		this.engineScope = new BindingsImpl(model);
		this.errorWriter = parent.getErrorWriter();
		this.globalScope = parent.getBindings(GLOBAL_SCOPE);
		this.reader = parent.getReader();
		this.writer = parent.getWriter();
	}

	@Override
	public Object getAttribute(String name) {
		if (engineScope.containsKey(name)) {
			return getAttribute(name, ENGINE_SCOPE);
		} else if (globalScope != null && globalScope.containsKey(name)) {
			return getAttribute(name, GLOBAL_SCOPE);
		}
		return null;
	}

	@Override
	public Object getAttribute(String name, int scope) {
		switch (scope) {

		case ENGINE_SCOPE:
			return engineScope.get(name);

		case GLOBAL_SCOPE:
			if (globalScope != null) {
				return globalScope.get(name);
			}
			return null;

		default:
			throw new IllegalArgumentException("Illegal scope value.");
		}
	}

	@Override
	public int getAttributesScope(String name) {
		if (engineScope.containsKey(name)) {
			return ENGINE_SCOPE;
		} else if (globalScope != null && globalScope.containsKey(name)) {
			return GLOBAL_SCOPE;
		} else {
			return -1;
		}
	}

	@Override
	public Bindings getBindings(int scope) {
		if (scope == ScriptContext.ENGINE_SCOPE) {
			return engineScope;
		} else if (scope == ScriptContext.GLOBAL_SCOPE) {
			return globalScope;
		}
		return null;
	}

	@Override
	public Writer getErrorWriter() {
		return errorWriter;
	}

	@Override
	public Reader getReader() {
		return reader;
	}

	@Override
	public List<Integer> getScopes() {
		return scopes;
	}

	@Override
	public Writer getWriter() {
		return writer;
	}

	@Override
	public Object removeAttribute(String name, int scope) {
		switch (scope) {

		case ENGINE_SCOPE:
			if (getBindings(ENGINE_SCOPE) != null) {
				return getBindings(ENGINE_SCOPE).remove(name);
			}
			return null;

		case GLOBAL_SCOPE:
			if (getBindings(GLOBAL_SCOPE) != null) {
				return getBindings(GLOBAL_SCOPE).remove(name);
			}
			return null;

		default:
			throw new IllegalArgumentException("Illegal scope value.");
		}
	}

	@Override
	public void setAttribute(String name, Object value, int scope) {
		switch (scope) {

		case ENGINE_SCOPE:
			engineScope.put(name, value);
			return;

		case GLOBAL_SCOPE:
			if (globalScope != null) {
				globalScope.put(name, value);
			}
			return;

		default:
			throw new IllegalArgumentException("Illegal scope value.");
		}

	}

	@Override
	public void setBindings(Bindings bindings, int scope) {
		switch (scope) {
		case ENGINE_SCOPE:
			if (bindings == null) {
				throw new NullPointerException("Engine scope cannot be null.");
			}
			engineScope = bindings;
			break;
		case GLOBAL_SCOPE:
			globalScope = bindings;
			break;
		default:
			throw new IllegalArgumentException("Invalid scope value.");
		}
	}

	@Override
	public void setErrorWriter(Writer writer) {
		this.errorWriter = writer;
	}

	@Override
	public void setReader(Reader reader) {
		this.reader = reader;
	}

	@Override
	public void setWriter(Writer writer) {
		this.writer = writer;
	}

}
