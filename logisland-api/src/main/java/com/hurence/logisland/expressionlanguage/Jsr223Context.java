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
package com.hurence.logisland.expressionlanguage;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.SimpleScriptContext;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

/**
 * Decorator for ScriptContext class.
 * The ONLY goal is to not leak JSR 223 outside of this package
 */
public class Jsr223Context implements ScriptContext {

    private final ScriptContext context;

    public Jsr223Context(){
        context = new SimpleScriptContext();
    }

    @Override
    public void setBindings(Bindings bindings, int i) {
        context.setBindings(bindings, i);
    }

    @Override
    public Bindings getBindings(int i) {
        return context.getBindings(i);
    }

    @Override
    public void setAttribute(String s, Object o, int i) {
        context.setAttribute(s, o, i);
    }

    @Override
    public Object getAttribute(String s, int i) {
        return context.getAttribute(s, i);
    }

    @Override
    public Object removeAttribute(String s, int i) {
        return context.removeAttribute(s, i);
    }

    @Override
    public Object getAttribute(String s) {
        return context.getAttribute(s);
    }

    @Override
    public int getAttributesScope(String s) {
        return context.getAttributesScope(s);
    }

    @Override
    public Writer getWriter() {
        return context.getWriter();
    }

    @Override
    public Writer getErrorWriter() {
        return context.getErrorWriter();
    }

    @Override
    public void setWriter(Writer writer) {
        context.setWriter(writer);
    }

    @Override
    public void setErrorWriter(Writer writer) {
        context.setErrorWriter(writer);
    }

    @Override
    public Reader getReader() {
        return context.getReader();
    }

    @Override
    public void setReader(Reader reader) {
        context.setReader(reader);
    }

    @Override
    public List<Integer> getScopes() {
        return context.getScopes();
    }
}
