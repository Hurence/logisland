/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.alerting;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.processor.datastore.MockDatastoreService;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class CheckAlertsTest {


    @Test
    public void testSyntax() throws InitializationException {

        // create the controller service and link it to the test processor
        final DatastoreClientService service = new MockDatastoreService();
        getCacheRecords().forEach(r -> service.put("test", r, false));

        final TestRunner runner = TestRunners.newTestRunner(CheckAlerts.class);
        runner.setProperty(CheckAlerts.MAX_CPU_TIME, "100");
        runner.setProperty(CheckAlerts.MAX_MEMORY, "12800000");
        runner.setProperty(CheckAlerts.MAX_PREPARED_STATEMENTS, "100");
        runner.setProperty(CheckAlerts.ALLOw_NO_BRACE, "false");
        runner.setProperty(CheckAlerts.PROFILE_ACTIVATION_CONDITION, "cache(\"cached_id1\").value > 10.0 && cache(\"cached_id2\").value >= 0");
        runner.setProperty("avib4", "cache(\"noone\").value < 5");             // syntax error
        runner.setProperty("avib5", "brousoufparty++++");                      // syntax error


        runner.setProperty(ComputeTags.DATASTORE_CLIENT_SERVICE, service.getIdentifier());
        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);

        runner.assertValid();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(0);
        runner.assertOutputErrorCount(2);

         }

    @Test
    public void testValueRules() throws InitializationException {

        // create the controller service and link it to the test processor
        final DatastoreClientService service = new MockDatastoreService();
        getCacheRecords().forEach(r -> service.put("test", r, false));

        final TestRunner runner = TestRunners.newTestRunner(CheckAlerts.class);
        runner.setProperty(CheckAlerts.MAX_CPU_TIME, "100");
        runner.setProperty(CheckAlerts.MAX_MEMORY, "12800000");
        runner.setProperty(CheckAlerts.MAX_PREPARED_STATEMENTS, "100");
        runner.setProperty(CheckAlerts.ALLOw_NO_BRACE, "false");
        runner.setProperty(CheckAlerts.PROFILE_ACTIVATION_CONDITION, "cache(\"cached_id1\").value > 10.0 && cache(\"cached_id2\").value >= 0");
        runner.setProperty("avib1", "cache(\"cached_id1\").value > 12.0");     // ok
        runner.setProperty("avib2", "cache(\"cached_id2\").value < 5");        // ok
        runner.setProperty("avib3", "cache(\"cached_id3\").value < 5");        // ko

        runner.setProperty(ComputeTags.DATASTORE_CLIENT_SERVICE, service.getIdentifier());
        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);

        runner.assertValid();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(2);
        runner.assertOutputErrorCount(0);

        for (Record enriched : runner.getOutputRecords()) {
            if (enriched.getId().equals("avib1")) {
                assertEquals(enriched.getField(FieldDictionary.RECORD_VALUE).asString(), "cache(\"cached_id1\").value > 12.0");
            }
        }
    }


    @Test
    public void testCountRules() throws InitializationException {

        // create the controller service and link it to the test processor
        final DatastoreClientService service = new MockDatastoreService();
        getCacheRecords().forEach(r -> service.put("test", r, false));

        final TestRunner runner = TestRunners.newTestRunner(CheckAlerts.class);
        runner.setProperty(CheckAlerts.MAX_CPU_TIME, "100");
        runner.setProperty(CheckAlerts.MAX_MEMORY, "12800000");
        runner.setProperty(CheckAlerts.MAX_PREPARED_STATEMENTS, "100");
        runner.setProperty(CheckAlerts.ALLOw_NO_BRACE, "false");
        runner.setProperty(CheckAlerts.PROFILE_ACTIVATION_CONDITION, "cache(\"cached_id1\").value > 10.0 && cache(\"cached_id2\").value >= 0");
        runner.setProperty("avib6", "cache(\"cached_id1\").count > 4");        // ok

        runner.setProperty(ComputeTags.DATASTORE_CLIENT_SERVICE, service.getIdentifier());
        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);

        runner.assertValid();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(1);
        runner.assertOutputErrorCount(0);

    }

    @Test
    public void testDurationRules() throws InitializationException {

        // create the controller service and link it to the test processor
        final DatastoreClientService service = new MockDatastoreService();
        getCacheRecords().forEach(r -> service.put("test", r, false));

        final TestRunner runner = TestRunners.newTestRunner(CheckAlerts.class);
        runner.setProperty(CheckAlerts.MAX_CPU_TIME, "100");
        runner.setProperty(CheckAlerts.MAX_MEMORY, "12800000");
        runner.setProperty(CheckAlerts.MAX_PREPARED_STATEMENTS, "100");
        runner.setProperty(CheckAlerts.ALLOw_NO_BRACE, "false");
        runner.setProperty(CheckAlerts.PROFILE_ACTIVATION_CONDITION, "cache(\"cached_id1\").value > 10.0 && cache(\"cached_id2\").value >= 0");
        runner.setProperty("avib7", "cache(\"cached_id2\").duration > 10000"); // ok

        runner.setProperty(ComputeTags.DATASTORE_CLIENT_SERVICE, service.getIdentifier());
        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);

        runner.assertValid();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(1);
        runner.assertOutputErrorCount(0);

    }


    private Collection<Record> getCacheRecords() {
        Collection<Record> lookupRecords = new ArrayList<>();

        Long startTime = System.currentTimeMillis() - 30000; // 30" ago

        lookupRecords.add(new StandardRecord()
                .setId("cached_id1")
                .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 12.45)
                .setField(FieldDictionary.RECORD_COUNT, FieldType.LONG, 5L));

        lookupRecords.add(new StandardRecord()
                .setId("cached_id2")
                .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 2.5)
                .setTime(startTime));

        lookupRecords.add(new StandardRecord()
                .setId("cached_id3")
                .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 5.5));

        return lookupRecords;
    }
}
