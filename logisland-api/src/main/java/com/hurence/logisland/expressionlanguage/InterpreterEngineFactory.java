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


/**
 * Created by mathieu on 01/06/17.
 */
public class InterpreterEngineFactory {

    private static final String defaultEngineName = "mvel";
    //private static final String defaultEngineName = "javascript";

    private static Jsr223InterpreterEngine singleton;
    private static final Object lock = new Object();

    public static InterpreterEngine get() {
        if (singleton == null) {
            synchronized (lock) {
                if (singleton == null) {
                    singleton = new Jsr223InterpreterEngine(defaultEngineName);
                }
            }
        }
        return singleton;
    }

    public static Jsr223InterpreterEngine setInterpreter(String interpreter){
        synchronized (lock) {
            if ((singleton == null ) ||
                    (! singleton.getName().equals(interpreter)))
            singleton = new Jsr223InterpreterEngine(interpreter);
        }
        return singleton;
    }

    // Prevent instanciation
    private InterpreterEngineFactory(){}
}
