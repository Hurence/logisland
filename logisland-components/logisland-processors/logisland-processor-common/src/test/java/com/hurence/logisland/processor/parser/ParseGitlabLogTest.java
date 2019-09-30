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
package com.hurence.logisland.processor.parser;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test simple Gitlab logs processor.
 */
public class ParseGitlabLogTest {
    
    private static Logger logger = LoggerFactory.getLogger(ParseGitlabLogTest.class);
    
    // Gitlab input event
    private static final String GITLAB_EVENT =
        "{" +
                "\"view\": 94.68," +
                "\"method\": \"GET\"," +
                "\"path\": \"/dashboard/issues\"" +
        "}";

    // Fake deep input event
    private static final String FAKE_DEEP_EVENT =
        "{" +
                "\"ts\": 27," +
                "\"uid\": \"anId\"," +
                "\"level.a1\": {\"level.a2a\": \"level.a2a.value\"," +
                "               \"level.a2b\": \"level.a2b.value\"}," +
                "\"int\": 123," +
                "\"level.b1\": {\"level.b2a\": {\"level.b3a\": \"level.b3a.value\"," +
                "                               \"level.b3b\": \"level.b3b.value\"}," +
                "               \"level.b2b\": [\"level.b2b.value1\", \"level.b2b.value2\"]}," +
                "\"booleanT\": true," +
                "\"booleanF\": false," +
                "\"float\": 123.456," +
                "\"long\": 32345678910" +
        "}";


    // Gitlab input event with param
    private static final String GITLAB_EVENT_WITH_PARAMS =
            "{" +
                "\"view\": 94.68," +
                "\"method\": \"GET\"," +
                "\"path\": \"/dashboard/issues\"," +
                "\"params\": [" +
                    "{" +
                        "\"key\": \"utf8\"," +
                        "\"value\": \"✓\"" +
                    "}," +
                    "{" +
                        "\"key\": \"authenticity_token\"," +
                        "\"value\": \"[FILTERED]\"" +
                    "}," +
                    "{" +
                        "\"key\": \"user\"," +
                        "\"value\": {" +
                                        "\"login\": \"mathieu.rossignol@hurence.com\"," +
                                        "\"password\": \"[FILTERED]\"," +
                                        "\"remember_me\": \"0\"" +
                                    "}" +
                    "}," +
                    "{" +
                        "\"key\": \"empty\"," +
                        "\"value\": \"\"" +
                    "}," +
                    "{" +
                        "\"key\": \"null\"," +
                        "\"value\": null" +
                    "}," +
                    "{" +
                         "\"key\": \"integer\"," +
                         "\"value\": 7" +
                    "}" +
                "]" +
            "}";

    /**
     * Test fields renaming if deep JSON and also some types
     */
    @Test
    public void testFakeDeepEvent() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParseGitlabLog());
        testRunner.assertValid();
        Record record = new StandardRecord("gitlab_event");
        record.setStringField(FieldDictionary.RECORD_VALUE, FAKE_DEEP_EVENT);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        
        out.assertFieldExists(FieldDictionary.RECORD_TYPE);

        out.assertFieldExists("ts");
        out.assertFieldEquals("ts", 27);
        
        out.assertFieldExists("uid");
        out.assertFieldEquals("uid", "anId");

        out.assertFieldExists("level_a1");
        Map<String, Object> level_a1 = (Map<String, Object>)out.getField("level_a1").getRawValue();
        String level_a2aValue = (String)level_a1.get("level_a2a");
        assertEquals("level.a2a.value", level_a2aValue);
        String level_a2bValue = (String)level_a1.get("level_a2b");
        assertEquals("level.a2b.value", level_a2bValue);

        out.assertFieldExists("int");
        out.assertFieldEquals("int", (int)123);

        out.assertFieldExists("level_b1");
        Map<String, Object> level_b1 = (Map<String, Object>)out.getField("level_b1").getRawValue();
        Map<String, Object> level_b2aValue = (Map<String, Object>)level_b1.get("level_b2a");
        String level_b3aValue = (String)level_b2aValue.get("level_b3a");
        assertEquals("level.b3a.value", level_b3aValue);
        String level_b3bValue = (String)level_b2aValue.get("level_b3b");
        assertEquals("level.b3b.value", level_b3bValue);
        List<String> level_b2bValue = (List<String>)level_b1.get("level_b2b");
        assertEquals(Arrays.asList("level.b2b.value1", "level.b2b.value2"), level_b2bValue);

        out.assertFieldExists("booleanT");
        out.assertFieldEquals("booleanT", true);

        out.assertFieldExists("booleanF");
        out.assertFieldEquals("booleanF", false);

        out.assertFieldExists("float");
        out.assertFieldEquals("float", (float)123.456);

        out.assertFieldExists("long");
        out.assertFieldEquals("long", (long)32345678910L);
    }

    @Test
    public void testGitlabLog() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParseGitlabLog());
        testRunner.assertValid();
        Record record = new StandardRecord("gitlab_event");
        record.setStringField(FieldDictionary.RECORD_VALUE, GITLAB_EVENT);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        
        out.assertFieldExists(FieldDictionary.RECORD_TYPE);

        out.assertFieldExists("view");
        out.assertFieldEquals("view", (float)94.68);

        out.assertFieldExists("method");
        out.assertFieldEquals("method", "GET");

        out.assertFieldExists("path");
        out.assertFieldEquals("path", "/dashboard/issues");
    }

    /**
     * Test that the special params field as been exploded and replaced with first level fields
     */
    @Test
    public void testFlatParams() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ParseGitlabLog());
        testRunner.assertValid();
        Record record = new StandardRecord("gitlab_event");
        record.setStringField(FieldDictionary.RECORD_VALUE, GITLAB_EVENT_WITH_PARAMS);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists(FieldDictionary.RECORD_TYPE);

        out.assertFieldExists("view");
        out.assertFieldEquals("view", (float)94.68);

        out.assertFieldExists("method");
        out.assertFieldEquals("method", "GET");

        out.assertFieldExists("path");
        out.assertFieldEquals("path", "/dashboard/issues");

        out.assertFieldExists("params_utf8");
        out.assertFieldEquals("params_utf8", "✓");

        out.assertFieldExists("params_authenticity_token");
        out.assertFieldEquals("params_authenticity_token", "[FILTERED]");

        out.assertFieldExists("params_user_login");
        out.assertFieldEquals("params_user_login", "mathieu.rossignol@hurence.com");

        out.assertFieldExists("params_user_password");
        out.assertFieldEquals("params_user_password", "[FILTERED]");

        out.assertFieldExists("params_empty");
        out.assertFieldEquals("params_empty", "");

        out.assertFieldExists("params_null");
        out.assertNullField("params_null");

        out.assertFieldExists("params_integer");
        out.assertFieldEquals("params_integer", 7);

        System.out.println(out);
    }
}
