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

import com.hurence.logisland.jsr223.BindingsImpl;
import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestMvelEngine {

    @Test
    public void test() {
        ScriptEngineManager sem = new ScriptEngineManager();
        ScriptEngine se = sem.getEngineByName("mvel");
        Map<String, Object> model = new HashMap<>();
        model.put("property", "value 1");
        model.put("myMap", Collections.singletonMap("k", "v"));
        try {
            Object result = se.eval("property", new BindingsImpl(model));
            assertNotNull("value 1", result);
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testMapAccess() throws Exception {
        ScriptEngineManager sem = new ScriptEngineManager();
        ScriptEngine se = sem.getEngineByName("mvel");
        Map<String, Object> model = new HashMap<>();
        model.put("myMap", Collections.singletonMap("k", "v"));

        Object result = se.eval("myMap['k']", new BindingsImpl(model));
        assertEquals("v", result);


    }


}
