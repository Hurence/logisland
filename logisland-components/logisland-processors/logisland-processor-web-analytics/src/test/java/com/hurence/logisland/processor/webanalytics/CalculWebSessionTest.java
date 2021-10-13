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
package com.hurence.logisland.processor.webanalytics;

import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.jupiter.api.Test;

import static com.hurence.logisland.processor.webanalytics.util.ElasticsearchServiceUtil.EVENT_INDEX_PREFIX;
import static com.hurence.logisland.processor.webanalytics.util.ElasticsearchServiceUtil.SESSION_INDEX_PREFIX;

public class CalculWebSessionTest {

    @Test
    public void testValidity()
    {
        CalculWebSession proc = new CalculWebSession();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.assertNotValid();

        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM");
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd");
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.assertValid();

        runner.removeProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.removeProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.removeProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM");
        runner.removeProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd");
        runner.removeProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.removeProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.assertValid();

        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "aba");
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "Canada/Yukon");
        runner.assertValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "CaNAda/YuKON");
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "UTC");
        runner.assertValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "Japan");
        runner.assertValid();
    }

    @Test
    public void testValidity2()
    {
        CalculWebSession proc = new CalculWebSession();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.assertNotValid();

        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, "TODO");
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, "TODO");
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, "TODO");
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.assertNotValid();
    }
}
