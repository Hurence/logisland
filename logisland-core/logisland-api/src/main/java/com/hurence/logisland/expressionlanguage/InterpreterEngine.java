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
package com.hurence.logisland.expressionlanguage;

import javax.script.ScriptContext;
import javax.script.Compilable;
import javax.script.CompiledScript;

/**
 * Created by mathieu on 31/05/17.
 */
public interface InterpreterEngine {

    public String process(String script) throws InterpreterEngineException;
    public String process(String script, ScriptContext context) throws InterpreterEngineException;

    public boolean isCompilable();
    public CompiledScript compile(String script);

    // Handling of : ${...}
    static boolean isExpressionLanguage(String content) {
        if (content != null) {
            String trim = content.trim();
            if (trim.startsWith("${") && trim.endsWith("}")) {
                return true;
            }
        }
        return false;
    }

    static  String extractExpressionLanguage(String content) throws InterpreterEngineException {
        if (content == null) {
            throw new InterpreterEngineException("Wrong content (null)");
        }

        String trim = content.trim();
        if (trim.length() <= 3) {
            throw new InterpreterEngineException("Wrong content (too short) : " + trim);
        }

        if (trim.startsWith("${") && trim.endsWith("}")) {
            return trim.substring(2, trim.length() - 1);
        }
        throw new InterpreterEngineException("Wrong content (not using ${...}) : " + trim);
    }
}
